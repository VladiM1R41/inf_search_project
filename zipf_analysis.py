import json
import collections
import math
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def analyze_zipf(tokens_dir: str, output_dir: str = "zipf_analysis"):
    token_counter = collections.Counter()
    token_files = list(Path(tokens_dir).glob("*.tokens"))
    
    for i, file_path in enumerate(token_files):
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                token = line.strip().split()[0]
                if token:
                    token_counter[token] += 1
        
        if (i + 1) % 1000 == 0:
            print(f"Обработано {i+1}/{len(token_files)} файлов...")

    sorted_tokens = token_counter.most_common()
    ranks = range(1, len(sorted_tokens) + 1)
    frequencies = [count for _, count in sorted_tokens]
    log_ranks = [math.log(r) for r in ranks]
    log_freqs = [math.log(f) for f in frequencies]
    coeffs = np.polyfit(log_ranks, log_freqs, 1)
    s = -coeffs[0]
    C = math.exp(coeffs[1])

    Path(output_dir).mkdir(exist_ok=True)
    
    with open(f"{output_dir}/zipf_data.csv", 'w', encoding='utf-8') as f:
        f.write("rank,token,frequency,log_rank,log_frequency\n")
        for i, (token, freq) in enumerate(sorted_tokens):
            f.write(f"{i+1},{token},{freq},{log_ranks[i]},{log_freqs[i]}\n")

    results = {
        "total_unique_tokens": len(sorted_tokens),
        "zipf_parameter_s": float(s),
        "zipf_constant_C": float(C),
        "most_common_tokens": sorted_tokens[:50],
        "least_common_tokens": sorted_tokens[-50:]
    }
    
    with open(f"{output_dir}/zipf_results.json", 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    plt.figure(figsize=(12, 6))
    
    plt.subplot(1, 2, 1)
    plt.loglog(ranks, frequencies, 'b.', alpha=0.5, label='Эмпирические данные')
    plt.loglog(ranks, [C / (r ** s) for r in ranks], 'r-', 
               label=f'Закон Ципфа: f = {C:.2f}/r^{s:.3f}')
    plt.xlabel('log(Ранг)')
    plt.ylabel('log(Частота)')
    plt.title('Закон Ципфа (логарифмическая шкала)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    plt.subplot(1, 2, 2)
    plt.plot(ranks[:100], frequencies[:100], 'b.-')
    plt.xlabel('Ранг (топ-100)')
    plt.ylabel('Частота')
    plt.title('Топ-100 самых частых токенов')
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/zipf_plot.png", dpi=150)
    plt.show()
    
    print(f"\n=== РЕЗУЛЬТАТЫ АНАЛИЗА ЦИПФА ===")
    print(f"Уникальных токенов: {len(sorted_tokens):,}")
    print(f"Параметр s (Ципфа): {s:.4f}")
    print(f"Константа C: {C:.2f}")
    print(f"Самый частый токен: '{sorted_tokens[0][0]}' ({sorted_tokens[0][1]:,} раз)")
    
    return results

if __name__ == "__main__":
    analyze_zipf("tokens")