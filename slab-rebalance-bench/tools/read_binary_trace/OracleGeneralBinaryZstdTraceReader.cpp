#include "OracleGeneralBinaryZstdTraceReader.h"
#include <fstream>

ZstdReader *createZstdReader(const char *tracePath) {
  ZstdReader *reader = (ZstdReader *)malloc(sizeof(ZstdReader));
  if (!reader) {
    fprintf(stderr, "Failed to allocate memory for ZstdReader\n");
    return NULL;
  }

  reader->inputFile = fopen(tracePath, "rb");
  if (reader->inputFile == NULL) {
    fprintf(stderr, "Cannot open %s\n", tracePath);
    free(reader);
    return NULL;
  }

  reader->bufferInSize = ZSTD_DStreamInSize();
  reader->bufferIn = malloc(reader->bufferInSize);
  if (!reader->bufferIn) {
    fprintf(stderr, "Failed to allocate memory for bufferIn\n");
    fclose(reader->inputFile);
    free(reader);
    return NULL;
  }

  reader->bufferOutSize = ZSTD_DStreamOutSize() * 2;
  reader->bufferOut = malloc(reader->bufferOutSize);
  if (!reader->bufferOut) {
    fprintf(stderr, "Failed to allocate memory for bufferOut\n");
    free(reader->bufferIn);
    fclose(reader->inputFile);
    free(reader);
    return NULL;
  }

  reader->input.src = reader->bufferIn;
  reader->input.size = 0;
  reader->input.pos = 0;

  reader->output.dst = reader->bufferOut;
  reader->output.size = reader->bufferOutSize;
  reader->output.pos = 0;

  reader->bufferOutReadPos = 0;
  reader->status = OK;
  reader->itemSize = 24;
  reader->ignoreSizeZeroReq = 1;
  reader->readDirection = READ_FORWARD;

  reader->zds = ZSTD_createDStream();
  if (!reader->zds) {
    fprintf(stderr, "Failed to create ZSTD_DStream\n");
    free(reader->bufferIn);
    free(reader->bufferOut);
    fclose(reader->inputFile);
    free(reader);
    return NULL;
  }

  size_t initResult = ZSTD_initDStream(reader->zds);
  if (ZSTD_isError(initResult)) {
    fprintf(stderr, "ZSTD_initDStream error: %s\n", ZSTD_getErrorName(initResult));
    ZSTD_freeDStream(reader->zds);
    free(reader->bufferIn);
    free(reader->bufferOut);
    fclose(reader->inputFile);
    free(reader);
    return NULL;
  }

  fprintf(stderr, "create zstd reader %s\n", tracePath);
  return reader;
}

void freeZstdReader(ZstdReader *reader) {
  ZSTD_freeDStream(reader->zds);
  free(reader->bufferIn);
  free(reader->bufferOut);
  fclose(reader->inputFile);
  free(reader);
  fprintf(stderr, "free zstd reader\n");
}

size_t readFromFile(ZstdReader *reader) {
    size_t readSize = fread(reader->bufferIn, 1, reader->bufferInSize, reader->inputFile);
    if (readSize < reader->bufferInSize) {
        if (feof(reader->inputFile)) {
            reader->status = MY_EOF;
        } else {
            assert(ferror(reader->inputFile));
            reader->status = ERR;
            return 0;
        }
    }
    reader->input.size = readSize;
    reader->input.pos = 0;
    return readSize;
}

rstatus decompressFromBuffer(ZstdReader *reader) {
    void *bufferStart = (char *)reader->bufferOut + reader->bufferOutReadPos;
    size_t bufferLeftSize = reader->output.pos - reader->bufferOutReadPos;
    memmove(reader->bufferOut, bufferStart, bufferLeftSize);
    reader->output.pos = bufferLeftSize;
    reader->bufferOutReadPos = 0;
    size_t oldPos = bufferLeftSize;
  
    if (reader->input.pos >= reader->input.size) {
        size_t readSize = readFromFile(reader);
        if (readSize == 0) {
            if (reader->status == MY_EOF) {
                return MY_EOF;
            } else {
                fprintf(stderr, "read from file error\n");
                return ERR;
            }
        }
    }
  
    size_t const ret = ZSTD_decompressStream(reader->zds, &(reader->output), &(reader->input));
    if (ret != 0) {
        if (ZSTD_isError(ret)) {
            fprintf(stderr, "zstd decompression error: %s\n", ZSTD_getErrorName(ret));
            return ERR;
        }
    }
    return OK;
}

size_t zstdReaderReadBytes(ZstdReader *reader, size_t nByte, char **dataStart) {
    size_t size = 0;
    while (reader->bufferOutReadPos + nByte > reader->output.pos) {
        rstatus status = decompressFromBuffer(reader);
        if (status != OK) {
            if (status != MY_EOF) {
                fprintf(stderr, "error decompress file\n");
            } else {
                return 0;
            }
            break;
        }
    }
    if (reader->bufferOutReadPos + nByte <= reader->output.pos) {
        size = nByte;
        *dataStart = ((char *)reader->bufferOut) + reader->bufferOutReadPos;
        reader->bufferOutReadPos += nByte;
        return size;
    } else {
        fprintf(stderr, "do not have enough bytes %zu < %zu\n", reader->output.pos - reader->bufferOutReadPos, nByte);
        return size;
    }
}

static inline char *readBytesZstd(ZstdReader *reader, size_t size) {
    char *start;
    size_t sz = zstdReaderReadBytes(reader, size, &start);
    if (sz == 0) {
        if (reader->status != MY_EOF) {
            fprintf(stderr, "fail to read zstd trace\n");
        }
        return NULL;
    }
    return start;
}

int oracleGeneralBinReadOneReq(ZstdReader *reader, OracleGeneralBinRequest *req) {
    char *record = readBytesZstd(reader, reader->itemSize);
    if (record == NULL) {
        req->valid = false;
        return 1;
    }

    req->clockTime = *(uint32_t *)record;
    req->objId = *(uint64_t *)(record + 4);
    req->objSize = *(uint32_t *)(record + 12);
    req->nextAccessVtime = *(int64_t *)(record + 16);
    if (req->nextAccessVtime == -1 || req->nextAccessVtime == INT64_MAX) {
        req->nextAccessVtime = MAX_REUSE_DISTANCE;
    }

    if (req->objSize == 0 && reader->ignoreSizeZeroReq && reader->readDirection == READ_FORWARD) {
        return oracleGeneralBinReadOneReq(reader, req);
    }
    req->valid = true;
    return 0;
}


int main(int argc, char *argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <input_file_path> [output_file_path] [max_record_cnt] [print_min_max_size]" << std::endl;
        return 1;
    }

    const char *inputFilePath = argv[1];
    const char *outputFilePath = (argc >= 3 && std::string(argv[2]) != "print_min_max_size") ? argv[2] : nullptr;
    int maxRecordCnt = (argc >= 4 && std::string(argv[3]) != "print_min_max_size") ? std::atoi(argv[3]) : -1;
    bool printMinMaxSize = (argc >= 3 && std::string(argv[2]) == "print_min_max_size") || 
                           (argc >= 4 && std::string(argv[3]) == "print_min_max_size");

    ZstdReader *reader = createZstdReader(inputFilePath);
    if (!reader) {
        std::cerr << "Failed to create zstd reader" << std::endl;
        return 1;
    }

    std::ofstream outputFile;
    if (outputFilePath && !printMinMaxSize) {
        outputFile.open(outputFilePath);
        if (!outputFile.is_open()) {
            std::cerr << "Failed to open output file: " << outputFilePath << std::endl;
            freeZstdReader(reader);
            return 1;
        }
        outputFile << "clock_time,object_id,object_size,next_access_vtime\n";
    }

    OracleGeneralBinRequest req;
    int64_t recordCount = 0;
    int64_t minSize = INT64_MAX;
    int64_t maxSize = 0;
    int64_t smallObjCount = 0;
    uint32_t firstClockTime = 0;
    uint32_t lastClockTime = 0;
    bool firstReq = true;

    while (oracleGeneralBinReadOneReq(reader, &req) == 0) {
        if (firstReq) {
            firstClockTime = req.clockTime;
            firstReq = false;
        }
        lastClockTime = req.clockTime;

        if (printMinMaxSize) {
            if (req.objSize < minSize) minSize = req.objSize;
            if (req.objSize > maxSize) maxSize = req.objSize;
            if (req.objSize < 4 * 1024 * 1024) smallObjCount++;
        } else if (outputFilePath) {
            outputFile << req.clockTime << ","
                       << req.objId << ","
                       << req.objSize << ","
                       << req.nextAccessVtime << "\n";
        } else {
            std::cout << "Clock Time: " << req.clockTime << std::endl;
            std::cout << "Object ID: " << req.objId << std::endl;
            std::cout << "Object Size: " << req.objSize << std::endl;
            std::cout << "Next Access VTime: " << req.nextAccessVtime << std::endl;
            break;
        }
        recordCount++;
        if (maxRecordCnt != -1 && recordCount >= maxRecordCnt) {
            break;
        }
    }

    if (printMinMaxSize) {
        std::cout << "Min Object Size: " << minSize << std::endl;
        std::cout << "Max Object Size: " << maxSize << std::endl;
        std::cout << "Number of small records: " << smallObjCount << std::endl;
        std::cout << "Total number of records: " << recordCount << std::endl;
        std::cout << "First Clock Time: " << firstClockTime << std::endl;
        std::cout << "Last Clock Time: " << lastClockTime << std::endl;
        double duration = static_cast<double>(lastClockTime) - static_cast<double>(firstClockTime);
        double avgQps = (duration > 0) ? (static_cast<double>(recordCount) / duration) : 0.0;
        std::cout << "Average QPS: " << avgQps << std::endl;
    }

    if (outputFilePath && !printMinMaxSize) {
        outputFile.close();
    }

    freeZstdReader(reader);
    return 0;
}