
strategy_order = ["disabled", "tail-age", "eviction-rate", "hits", "lama", "marginal-hits", "marginal-hits-tuned"]
    

strategy_labels = {
    "disabled": r"$\mathit{Disabled}$",
    "tail-age": r"$\mathit{Tail\text{-}Age}$",
    "eviction-rate": r"$\mathit{Eviction\text{-}Rate}$",
    "hits": r"$\mathit{Hits\text{-}Per\text{-}Slab}$",
    "lama": r"$\mathit{LAMA}$",
    "marginal-hits": r"$\mathit{Marginal\text{-}Hits}$",
    "marginal-hits-tuned": r"$\mathit{Marginal\text{-}Hits\text{-}Tuned}$"
}

strategy_colors = {
    "disabled": "#636EFA",
    "tail-age": "#AB63FA", 
    "eviction-rate": "#FFA15A",
    "hits": "#00CC96",
    "lama": "#8C564B",  
    "marginal-hits": "#EF553B",
    "marginal-hits-tuned": "#2E8B57"
}

# Define line styles for variety
strategy_linestyles = {
    "disabled": '-',
    "tail-age": '--',
    "eviction-rate": '-.',
    "hits": ':',
    "lama": (0, (3, 1, 1, 1)),
    "marginal-hits": '--',
    "marginal-hits-tuned": '-'
}

# Define marker styles
strategy_markers = {
    "disabled": 'o',
    "tail-age": 's',
    "eviction-rate": '^',
    "hits": 'D',
    "lama": 'v',
    "marginal-hits": 'p',
    "marginal-hits-tuned": 'H'  
}

# Define allocator order and labels
allocator_order = ['LRU', 'LRU2Q', 'TINYLFU']
allocator_labels = ['LRU', 'TwoQ', 'TinyLFU']

# Define markers and line styles for allocators
allocator_markers = {
    'LRU': 'o',
    'LRU2Q': 's', 
    'TINYLFU': '^'
}

allocator_linestyles = {
    'LRU': '-',
    'LRU2Q': '--',
    'TINYLFU': '-.'
}

rcParams = {
    'font.size': 26,           # Increased from 20
    'axes.titlesize': 30,      # Increased from 24
    'axes.labelsize': 28,      # Increased from 22
    'xtick.labelsize': 26,     # Increased from 20
    'ytick.labelsize': 26,     # Increased from 20
    'legend.fontsize': 20,     # Increased from 18
    'figure.titlesize': 28,    # Increased from 26
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'axes.linewidth': 1.2,
    'grid.linewidth': 0.8,
    'grid.alpha': 0.3
}