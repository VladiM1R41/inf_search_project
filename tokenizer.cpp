#include <iostream>
#include <fstream>
#include <filesystem>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <chrono>
#include <iomanip>
#include <cctype>
#include <algorithm>
#include <locale>
#include <codecvt>

namespace fs = std::filesystem;


struct TokenizerConfig {
    bool lowercase = true;
    bool remove_numbers = true;
    bool remove_short_tokens = true; 
    size_t min_token_length = 2;
    bool save_positions = false; 
};

class UTF8Converter {
private:
    static const std::map<uint32_t, uint32_t> cyrillic_lowercase_map;
    
public:
    static void init_cyrillic_map() {
        static bool initialized = false;
        if (initialized) return;
        for (uint32_t upper = 0x0410; upper <= 0x042F; ++upper) {
            const_cast<std::map<uint32_t, uint32_t>&>(cyrillic_lowercase_map)[upper] = upper + 0x20;
        }
        const_cast<std::map<uint32_t, uint32_t>&>(cyrillic_lowercase_map)[0x0401] = 0x0451;
        const_cast<std::map<uint32_t, uint32_t>&>(cyrillic_lowercase_map)[0x0419] = 0x0439;
        
        initialized = true;
    }

    static std::string to_lower_rus_utf8(const std::string& utf8_str) {
        init_cyrillic_map();
        std::string result;
        result.reserve(utf8_str.size());
        
        for (size_t i = 0; i < utf8_str.size(); ) {
            unsigned char c = static_cast<unsigned char>(utf8_str[i]);
            
            if (c < 128) {
                if (c >= 'A' && c <= 'Z') {
                    result.push_back(c + 32);
                } else {
                    result.push_back(c);
                }
                i++;
            }
            else if ((c & 0xE0) == 0xC0) {
                if (i + 1 >= utf8_str.size()) {
                    result.push_back(c);
                    i++;
                    continue;
                }
                
                unsigned char c2 = static_cast<unsigned char>(utf8_str[i + 1]);
                uint32_t code_point = ((c & 0x1F) << 6) | (c2 & 0x3F);

                auto it = cyrillic_lowercase_map.find(code_point);
                if (it != cyrillic_lowercase_map.end()) {
                    uint32_t lower = it->second;
                    result.push_back(0xC0 | ((lower >> 6) & 0x1F));
                    result.push_back(0x80 | (lower & 0x3F));
                } else {
                    result.push_back(c);
                    result.push_back(c2);
                }
                i += 2;
            }
            else if ((c & 0xF0) == 0xE0) {
                size_t bytes = 3;
                if (i + bytes <= utf8_str.size()) {
                    result.append(utf8_str.substr(i, bytes));
                }
                i += bytes;
            }
            else if ((c & 0xF8) == 0xF0) {
                size_t bytes = 4;
                if (i + bytes <= utf8_str.size()) {
                    result.append(utf8_str.substr(i, bytes));
                }
                i += bytes;
            }
            else {
                result.push_back(c);
                i++;
            }
        }
        
        return result;
    }
    
    static bool is_utf8_letter_start(unsigned char c) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) return true;
        if (c == 0xD0 || c == 0xD1) return true;
        
        return false;
    }

    static bool is_word_continuation(unsigned char c) {
        return (c >= 'a' && c <= 'z') || 
               (c >= 'A' && c <= 'Z') ||
               (c >= '0' && c <= '9') ||
               (c == '_') || (c == '-') || (c == '\'');
    }
};

const std::map<uint32_t, uint32_t> UTF8Converter::cyrillic_lowercase_map = {
};

class ImprovedTokenizer {
public:
    ImprovedTokenizer(const std::string& input_dir, 
                     const std::string& output_dir,
                     const TokenizerConfig& config)
        : input_dir_(input_dir), output_dir_(output_dir), config_(config) {
        
        fs::create_directory(output_dir);
        UTF8Converter::init_cyrillic_map();
    }

    void process_all() {
        auto start_time = std::chrono::high_resolution_clock::now();

        std::vector<fs::path> txt_files;
        for (const auto& entry : fs::directory_iterator(input_dir_)) {
            if (entry.path().extension() == ".txt") {
                txt_files.push_back(entry.path());
            }
        }
        
        std::cout << "Found " << txt_files.size() << " text files to process\n";
        
        size_t total_tokens = 0;
        size_t total_chars = 0;
        size_t processed_files = 0;

        for (size_t i = 0; i < txt_files.size(); ++i) {
            try {
                auto stats = process_file(txt_files[i]);
                total_tokens += stats.token_count;
                total_chars += stats.total_token_length;
                processed_files++;

                if (processed_files % 100 == 0) {
                    std::cout << "Processed " << processed_files 
                              << "/" << txt_files.size() << " files (" 
                              << (processed_files * 100 / txt_files.size()) << "%)\n";
                }
            } catch (const std::exception& e) {
                std::cerr << "Error processing " << txt_files[i].filename() 
                          << ": " << e.what() << "\n";
            }
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
                       (end_time - start_time);

        save_stats(total_tokens, total_chars, processed_files, duration.count());
    }
    
private:
    struct FileStats { 
        size_t token_count; 
        size_t total_token_length; 
    };
    
    struct TokenInfo {
        std::string text;
        size_t position;
    };

    FileStats process_file(const fs::path& file_path) {
        std::ifstream file(file_path, std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Cannot open file: " + file_path.string());
        }

        std::string content((std::istreambuf_iterator<char>(file)),
                           std::istreambuf_iterator<char>());
        file.close();

        std::vector<TokenInfo> tokens = tokenize_text(content);

        std::string token_filename = output_dir_ + "/" + 
                                     file_path.stem().string() + ".tokens";
        std::ofstream token_file(token_filename, std::ios::binary);
        
        if (!token_file.is_open()) {
            throw std::runtime_error("Cannot create token file: " + token_filename);
        }
        
        size_t total_length = 0;
        for (const auto& token : tokens) {
            if (config_.save_positions) {
                token_file << token.text << " " << token.position << "\n";
            } else {
                token_file << token.text << "\n";
            }
            total_length += token.text.length();
        }
        
        token_file.close();
        
        return {tokens.size(), total_length};
    }
    std::vector<TokenInfo> tokenize_text(const std::string& text) {
        std::vector<TokenInfo> tokens;
        std::string current_token;
        bool in_token = false;
        size_t position = 0;
        
        for (size_t i = 0; i < text.length(); ) {
            unsigned char c = static_cast<unsigned char>(text[i]);

            if (UTF8Converter::is_utf8_letter_start(c)) {
                if (!in_token) {
                    in_token = true;
                    current_token.clear();
                }
                size_t char_len = get_utf8_char_length(c);
                if (i + char_len <= text.length()) {
                    std::string utf8_char = text.substr(i, char_len);

                    if (config_.lowercase) {
                        utf8_char = UTF8Converter::to_lower_rus_utf8(utf8_char);
                    }
                    
                    current_token += utf8_char;
                    i += char_len;
                } else {
                    i++;
                }
                continue;
            }
            if (in_token && UTF8Converter::is_word_continuation(c)) {
                current_token += c;
                i++;
                continue;
            }

            if (in_token) {
                if (should_keep_token(current_token)) {
                    tokens.push_back({current_token, position});
                    position++;
                }
                
                in_token = false;
            }
            
            i++; 
        }
 
        if (in_token && should_keep_token(current_token)) {
            tokens.push_back({current_token, position});
        }
        
        return tokens;
    }

    size_t get_utf8_char_length(unsigned char first_byte) {
        if (first_byte < 128) return 1;
        if ((first_byte & 0xE0) == 0xC0) return 2;
        if ((first_byte & 0xF0) == 0xE0) return 3;
        if ((first_byte & 0xF8) == 0xF0) return 4;
        return 1;
    }

    bool should_keep_token(const std::string& token) {
        if (config_.remove_short_tokens && token.length() < config_.min_token_length) {
            return false;
        }

        if (config_.remove_numbers) {
            bool all_digits = true;
            for (char c : token) {
                if (!std::isdigit(static_cast<unsigned char>(c))) {
                    all_digits = false;
                    break;
                }
            }
            if (all_digits) return false;
        }
        
        return true;
    }

    void save_stats(size_t total_tokens, size_t total_chars, 
                   size_t processed_files, long long milliseconds) {
        std::ofstream stats_file("tokenization_stats.json");
        
        double avg_length = total_tokens > 0 ? 
            static_cast<double>(total_chars) / total_tokens : 0.0;
        double tokens_per_sec = milliseconds > 0 ? 
            total_tokens * 1000.0 / milliseconds : 0;
        double docs_per_sec = milliseconds > 0 ? 
            processed_files * 1000.0 / milliseconds : 0;
        
        stats_file << std::fixed << std::setprecision(2);
        stats_file << "{\n";
        stats_file << "  \"total_tokens\": " << total_tokens << ",\n";
        stats_file << "  \"average_token_length\": " << avg_length << ",\n";
        stats_file << "  \"processing_time_ms\": " << milliseconds << ",\n";
        stats_file << "  \"processing_time_sec\": " << milliseconds / 1000.0 << ",\n";
        stats_file << "  \"documents_processed\": " << processed_files << ",\n";
        stats_file << "  \"tokens_per_second\": " << tokens_per_sec << ",\n";
        stats_file << "  \"documents_per_second\": " << docs_per_sec << ",\n";
        stats_file << "  \"average_tokens_per_document\": " 
                  << (processed_files > 0 ? 
                      static_cast<double>(total_tokens) / processed_files : 0) << "\n";
        stats_file << "}\n";

        std::cout << "\n=== TOKENIZATION STATISTICS ===\n";
        std::cout << "Total tokens: " << total_tokens << "\n";
        std::cout << "Average token length: " << avg_length << " chars\n";
        std::cout << "Processing time: " << milliseconds / 1000.0 << " sec\n";
        std::cout << "Documents processed: " << processed_files << "\n";
        std::cout << "Speed: " << tokens_per_sec << " tokens/sec, " 
                  << docs_per_sec << " docs/sec\n";
    }
    
    std::string input_dir_;
    std::string output_dir_;
    TokenizerConfig config_;
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_dir> [options]\n";
        std::cerr << "Options:\n";
        std::cerr << "  --no-lowercase      : Do not convert to lowercase\n";
        std::cerr << "  --keep-numbers      : Keep number tokens\n";
        std::cerr << "  --save-positions    : Save token positions\n";
        std::cerr << "  --min-length N      : Minimum token length (default: 2)\n";
        std::cerr << "\nExample: " << argv[0] << " corpus tokens --min-length 3\n";
        return 1;
    }
    
    std::string input_dir = argv[1];
    std::string output_dir = argv[2];
    
    if (!fs::exists(input_dir)) {
        std::cerr << "Error: Input directory '" << input_dir << "' does not exist\n";
        return 1;
    }
    TokenizerConfig config;
    
    for (int i = 3; i < argc; ++i) {
        std::string arg = argv[i];
        
        if (arg == "--no-lowercase") {
            config.lowercase = false;
        } else if (arg == "--keep-numbers") {
            config.remove_numbers = false;
        } else if (arg == "--save-positions") {
            config.save_positions = true;
        } else if (arg == "--min-length" && i + 1 < argc) {
            config.min_token_length = std::stoi(argv[++i]);
        } else {
            std::cerr << "Warning: Unknown argument '" << arg << "'\n";
        }
    }
    
    std::cout << "Tokenizer configuration:\n";
    std::cout << "  Lowercase: " << (config.lowercase ? "YES" : "NO") << "\n";
    std::cout << "  Remove numbers: " << (config.remove_numbers ? "YES" : "NO") << "\n";
    std::cout << "  Save positions: " << (config.save_positions ? "YES" : "NO") << "\n";
    std::cout << "  Min token length: " << config.min_token_length << "\n";
    std::cout << std::endl;
    
    try {
        ImprovedTokenizer tokenizer(input_dir, output_dir, config);
        tokenizer.process_all();
        std::cout << "\nTokenization completed successfully!\n";
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}