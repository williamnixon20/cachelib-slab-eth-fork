#include <zstd.h>

#include <climits>
#include <cstdint>
#include <cstring> // std::memmove
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <vector>

// --- Record struct ---
struct OracleGeneralBinRequest {
  uint32_t clockTime;
  uint64_t objId;
  uint32_t objSize;
  int64_t nextAccessVtime;
};

// --- Reader class ---
class ZstdReader {
 public:
  ZstdReader(bool compressed = true);
  ~ZstdReader();

  void open(const std::string& path, bool compressed = true);
  void close();
  bool is_open() const;

  bool read_one_req(OracleGeneralBinRequest* req);

 private:
  std::ifstream ifile;
  bool compressed_;

  std::unique_ptr<ZSTD_DStream, decltype(&ZSTD_freeDStream)> zds;
  std::vector<char> buff_in, buff_out;
  size_t read_pos;

  ZSTD_inBuffer zin;
  ZSTD_outBuffer zout;

  enum class Status { OK, EOF_, ERR } status;

  size_t refill_in();
  Status decompress();
  size_t read_bytes(size_t n, char** out);
};

// --- Implementation ---

ZstdReader::ZstdReader(bool compressed)
    : compressed_(compressed),
      zds(compressed ? ZSTD_createDStream() : nullptr, ZSTD_freeDStream),
      buff_in(compressed ? ZSTD_DStreamInSize() : 0),
      buff_out(compressed ? ZSTD_DStreamOutSize() * 2 : 0),
      read_pos(0),
      status(Status::OK) {
  if (compressed_) {
    zin = {buff_in.data(), 0, 0};
    zout = {buff_out.data(), buff_out.size(), 0};
    ZSTD_initDStream(zds.get());
  }
}

ZstdReader::~ZstdReader() { close(); }

void ZstdReader::open(const std::string& path, bool compressed) {
  close();
  compressed_ = compressed;
  ifile.open(path, std::ios::binary);
  if (!ifile.is_open())
    throw std::runtime_error("Cannot open file: " + path);

  if (compressed_) {
    if (!zds)
      zds.reset(ZSTD_createDStream());
    buff_in.resize(ZSTD_DStreamInSize());
    buff_out.resize(ZSTD_DStreamOutSize() * 2);
    zin = {buff_in.data(), 0, 0};
    zout = {buff_out.data(), buff_out.size(), 0};
    read_pos = 0;
    ZSTD_initDStream(zds.get());
  }
}

bool ZstdReader::is_open() const { return ifile.is_open(); }

void ZstdReader::close() {
  if (ifile.is_open())
    ifile.close();
}

size_t ZstdReader::refill_in() {
  ifile.read(buff_in.data(), buff_in.size());
  size_t r = ifile.gcount();
  zin = {buff_in.data(), r, 0};
  if (r == 0)
    status = Status::EOF_;
  return r;
}

ZstdReader::Status ZstdReader::decompress() {
  // shift unread output to front
  memmove(buff_out.data(), buff_out.data() + read_pos, zout.pos - read_pos);
  zout.pos -= read_pos;
  read_pos = 0;

  if (zin.pos == zin.size && refill_in() == 0)
    return Status::EOF_;

  size_t ret = ZSTD_decompressStream(zds.get(), &zout, &zin);
  if (ZSTD_isError(ret)) {
    std::cerr << "ZSTD error: " << ZSTD_getErrorName(ret) << "\n";
    status = Status::ERR;
    return Status::ERR;
  }
  return Status::OK;
}

size_t ZstdReader::read_bytes(size_t n, char** out) {
  if (!compressed_) {
    static std::vector<char> buf;
    buf.resize(n);
    ifile.read(buf.data(), n);
    if ((size_t)ifile.gcount() != n)
      return 0;
    *out = buf.data();
    return n;
  }

  while (read_pos + n > zout.pos) {
    auto s = decompress();
    if (s != Status::OK)
      return 0;
  }
  *out = buff_out.data() + read_pos;
  read_pos += n;
  return n;
}

bool ZstdReader::read_one_req(OracleGeneralBinRequest* req) {
  char* p;
  if (read_bytes(24, &p) != 24)
    return false;
  req->clockTime = *(uint32_t*)p;
  req->objId = *(uint64_t*)(p + 4);
  req->objSize = *(uint32_t*)(p + 12);
  req->nextAccessVtime = *(int64_t*)(p + 16);

  if (req->nextAccessVtime == -1 || req->nextAccessVtime == LLONG_MAX)
    req->nextAccessVtime = LLONG_MAX;

  if (req->objSize == 0)
    return read_one_req(req);
  return true;
}

//////////////

#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional> // std::hash
#include <iomanip>    // for std::setprecision
#include <iostream>
#include <unordered_map>
#include <unordered_set>

static inline double toMBd(uint64_t bytes) {
  return double(bytes) / (1024.0 * 1024.0);
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: ./ws trace.bin[.zst]\n";
    return 1;
  }

  bool is_zstd = (std::string(argv[1]).find(".zst") != std::string::npos);
  ZstdReader r(is_zstd);
  r.open(argv[1], is_zstd);

  // --- input file size ---
  uint64_t file_size = 0;
  {
    std::ifstream in(argv[1], std::ifstream::ate | std::ifstream::binary);
    if (!in.is_open()) {
      std::cerr << "Cannot open file to get size: " << argv[1] << "\n";
      return 1;
    }
    file_size = static_cast<uint64_t>(in.tellg());
  }

  // --- sampling factor ---
  int scaling_factor = 1;
  const uint64_t GiB = 1024ULL * 1024ULL * 1024ULL;
  if (file_size > 5 * GiB) {
    scaling_factor = 21;
  } else if (file_size > 1 * GiB) {
    scaling_factor = 11;
  }

  uint64_t total_bytes = 0;
  uint64_t total_requests = 0;
  std::unordered_map<uint64_t, uint32_t> sampled_unique;
  sampled_unique.reserve(500000);

  std::hash<uint64_t> hfunc;
  OracleGeneralBinRequest req;

  while (r.read_one_req(&req)) {
    total_bytes += req.objSize;
    total_requests += 1;

    uint64_t h = hfunc(req.objId);
    if (scaling_factor > 1 && (h % scaling_factor) != 0)
      continue;

    sampled_unique[h] = req.objSize;
  }

  r.close();

  uint64_t sampled_unique_bytes = 0;
  for (auto& kv : sampled_unique) {
    sampled_unique_bytes += kv.second;
  }

  uint64_t estimated_unique_bytes = sampled_unique_bytes * scaling_factor;

  // --- Construct JSON in memory ---
  std::ostringstream json;
  std::string file_path = std::string(argv[1]);
  std::string file_name = std::filesystem::path(file_path).filename().string();
  json << std::fixed << std::setprecision(2);
  json << "{"
       << "\"file_path\":\"" << file_path << "\","
       << "\"file_name\":\"" << file_name << "\","
       << "\"file_size_bytes\":" << file_size << ","
       << "\"file_size_mb\":" << toMBd(file_size) << ","
       << "\"scaling_factor\":" << scaling_factor << ","
       << "\"total_requests\":" << total_requests << ","
       << "\"total_bytes_seen\":" << total_bytes << ","
       << "\"total_mb_seen\":" << toMBd(total_bytes) << ","
       << "\"unique_bytes_sampled\":" << sampled_unique_bytes << ","
       << "\"unique_mb_sampled\":" << toMBd(sampled_unique_bytes) << ","
       << "\"unique_bytes_estimated\":" << estimated_unique_bytes << ","
       << "\"unique_mb_estimated\":" << toMBd(estimated_unique_bytes) << "}";

  std::cout << json.str() << "\n";

  // --- Also dump to file: wss_calc_json/<basename>.ws.json ---
  std::string dir = "output_wss_calc";
  system(("mkdir -p " + dir).c_str());

  std::string out_file = dir + "/" + file_name + ".ws.json";
  std::ofstream ofs(out_file);
  if (ofs.is_open()) {
    ofs << json.str() << "\n";
    ofs.close();
    std::cerr << "[ws] JSON written to: " << out_file << "\n";
  } else {
    std::cerr << "[ws] ERROR: Could not write to " << out_file << "\n";
  }

  return 0;
}

// g++ -O3 -std=c++17 wss_zstd_calc.cpp -lzstd -o zstd_reader
// ./zstd_reader /mnt/data/privworkload/ftp.pdl.cmu.edu/pub/datasets/twemcacheWorkload/.priv/tencent_photo1.oracleGeneral.sample10.zst
