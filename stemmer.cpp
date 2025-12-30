#include <iostream>
#include <fstream>
#include <cstring>
#include <cctype>
#include <windows.h>
#include <io.h>

#define MAX_WORD_LEN 256
#define MAX_PATH_LEN 512

class RussianStemmer {
private:
    static void replace_yo_with_e(char* word) {
        size_t len = strlen(word);
        size_t i = 0;
        
        while (i < len) {
            unsigned char c = static_cast<unsigned char>(word[i]);
            if (c == 0xD1 && i + 1 < len && 
                static_cast<unsigned char>(word[i+1]) == 0x91) {
                word[i] = 0xD0;
                word[i+1] = 0xB5;
                i += 2;
            } 
            else if (c == 0xD0 && i + 1 < len && 
                     static_cast<unsigned char>(word[i+1]) == 0x81) {

                word[i] = 0xD0;
                word[i+1] = 0xB5;
                i += 2;
            }
            else {
                i++;
            }
        }
    }
    
    static void to_lower_russian(char* word) {
        size_t i = 0;
        size_t len = strlen(word);
        
        while (i < len) {
            unsigned char c = static_cast<unsigned char>(word[i]);
            
            if (c == 0xD0 && i + 1 < len) {
                unsigned char c2 = static_cast<unsigned char>(word[i+1]);
                if (c2 >= 0x90 && c2 <= 0xAF) {
                    word[i+1] = c2 + 0x20;
                }
                else if (c2 == 0x81) {
                    word[i+1] = 0xB5;
                }
                i += 2;
            }
            else if (c == 0xD1 && i + 1 < len) {
                unsigned char c2 = static_cast<unsigned char>(word[i+1]);
                
                i += 2;
            }
            else if (c >= 'A' && c <= 'Z') {
                word[i] = c + 32;
                i++;
            }
            else {
                i++;
            }
        }
    }

    static bool ends_with_utf8(const char* word, const char* suffix) {
        size_t word_len = strlen(word);
        size_t suffix_len = strlen(suffix);
        
        if (word_len < suffix_len) return false;
        
        return strcmp(word + word_len - suffix_len, suffix) == 0;
    }

    static void remove_suffix_utf8(char* word, const char* suffix) {
        size_t word_len = strlen(word);
        size_t suffix_len = strlen(suffix);
        
        if (word_len >= suffix_len && ends_with_utf8(word, suffix)) {
            word[word_len - suffix_len] = '\0';
        }
    }

    static bool is_number(const char* word) {
        for (size_t i = 0; word[i]; i++) {
            if (!isdigit(static_cast<unsigned char>(word[i]))) {
                return false;
            }
        }
        return true;
    }
    
public:
    static void stem_word(char* word) {
        if (strlen(word) == 0) return;

        to_lower_russian(word);
        replace_yo_with_e(word);
        
        size_t len = strlen(word);
        if (len < 3) return; 

        if (is_number(word)) {
            word[0] = '\0';
            return;
        }

        const char* noun_suffixes[] = {
            "ам", "ям", "ом", "ем", "ой", "ей",
            "ов", "ев", "ей", 
            "ами", "ями",
            "ах", "ях",
            NULL
        };
        
        for (int i = 0; noun_suffixes[i] != NULL; i++) {
            if (ends_with_utf8(word, noun_suffixes[i])) {
                remove_suffix_utf8(word, noun_suffixes[i]);
                len = strlen(word);
                if (len < 2) return;
                break;
            }
        }

        const char* verb_suffixes[] = {
            "ла", "ло", "ли",
            "ть",
            "лся", "лось", "лись",
            "ал", "ял", "ил", "ыл",
            NULL
        };
        
        for (int i = 0; verb_suffixes[i] != NULL; i++) {
            if (ends_with_utf8(word, verb_suffixes[i])) {
                remove_suffix_utf8(word, verb_suffixes[i]);
                len = strlen(word);
                if (len < 2) return;
                break;
            }
        }

        const char* adj_suffixes[] = {
            "ый", "ий", "ой",
            "ая", "яя",
            "ое", "ее",
            "ые", "ие",
            "ого", "его", 
            "ому", "ему", 
            NULL
        };
        
        for (int i = 0; adj_suffixes[i] != NULL; i++) {
            if (ends_with_utf8(word, adj_suffixes[i])) {
                remove_suffix_utf8(word, adj_suffixes[i]);
                len = strlen(word);
                if (len < 2) return;
                break;
            }
        }

        len = strlen(word);
        if (len >= 2) {
            unsigned char last1 = static_cast<unsigned char>(word[len-2]);
            unsigned char last2 = static_cast<unsigned char>(word[len-1]);
            
            if (last1 == 0xD1 && last2 == 0x8C) {
                word[len-2] = '\0';
            }
        }
        
    }

    static bool process_file(const char* input_file, const char* output_file) {
        std::ifstream infile(input_file, std::ios::binary);
        if (!infile) {
            std::cerr << "Cannot open: " << input_file << std::endl;
            return false;
        }
        
        std::ofstream outfile(output_file, std::ios::binary);
        if (!outfile) {
            std::cerr << "Cannot create: " << output_file << std::endl;
            return false;
        }
        
        char line[MAX_WORD_LEN];
        int token_count = 0;
        
        while (infile.getline(line, MAX_WORD_LEN)) {
            char* space_pos = strchr(line, ' ');
            if (space_pos) {
                *space_pos = '\0';
            }

            if (strlen(line) == 0) continue;

            char word[MAX_WORD_LEN];
            strcpy(word, line);
            
            stem_word(word);

            if (strlen(word) > 0) {
                outfile << word << '\n';
                token_count++;
            }
        }
        
        std::cout << "  -> " << token_count << " tokens" << std::endl;
        return true;
    }
    
    static bool process_directory(const char* input_dir, const char* output_dir) {
        if (!CreateDirectory(output_dir, NULL)) {
            DWORD err = GetLastError();
            if (err != ERROR_ALREADY_EXISTS) {
                std::cerr << "Cannot create directory: " << output_dir 
                          << " (error: " << err << ")" << std::endl;
                return false;
            }
        }

        char search_path[MAX_PATH_LEN];
        snprintf(search_path, sizeof(search_path), "%s\\*.tokens", input_dir);
        
        struct _finddata_t fileinfo;
        intptr_t handle = _findfirst(search_path, &fileinfo);
        
        if (handle == -1) {
            std::cerr << "No .tokens files found in: " << input_dir << std::endl;
            return false;
        }
        
        int total_files = 0;
        
        do {
            if (!(fileinfo.attrib & _A_SUBDIR)) {
                char input_path[MAX_PATH_LEN];
                char output_path[MAX_PATH_LEN];
                
                snprintf(input_path, sizeof(input_path), 
                        "%s\\%s", input_dir, fileinfo.name);
                snprintf(output_path, sizeof(output_path), 
                        "%s\\%s", output_dir, fileinfo.name);
                
                std::cout << fileinfo.name << "... ";
                
                if (process_file(input_path, output_path)) {
                    total_files++;
                }
            }
        } while (_findnext(handle, &fileinfo) == 0);
        
        _findclose(handle);
        std::cout << "\nTotal: " << total_files << " files processed" << std::endl;
        return true;
    }

    static void test() {
        std::cout << "=== ТЕСТ СТЕММЕРА ===" << std::endl;
        
        struct {
            const char* input;
            const char* expected;
        } tests[] = {
            {"столы", "стол"},
            {"книги", "книг"},
            {"красивый", "красив"},
            {"синий", "син"},
            {"делать", "дел"},
            {"говорил", "говор"},
            {"ёлка", "елк"},
            {"поезд", "поезд"},
            {"читал", "чит"},
            {"писала", "пис"},

            {"123", ""},
            {"2024", ""},

            {"он", "он"},
            {"я", "я"},
            
            {NULL, NULL}
        };
        
        int passed = 0;
        int total = 0;
        
        for (int i = 0; tests[i].input != NULL; i++) {
            char word[MAX_WORD_LEN];
            strcpy(word, tests[i].input);
            
            stem_word(word);
            
            bool correct = (strcmp(word, tests[i].expected) == 0);
            std::cout << (correct ? "✓ " : "✗ ");
            std::cout << tests[i].input << " -> \"" << word << "\"";
            
            if (!correct) {
                std::cout << " (expected: \"" << tests[i].expected << "\")";
            }
            std::cout << std::endl;
            
            if (correct) passed++;
            total++;
        }
        
        std::cout << "\nРезультат: " << passed << "/" << total 
                  << " (" << (passed * 100 / total) << "%)" << std::endl;
    }
};

int main(int argc, char* argv[]) {
    std::cout << "=== СТЕММЕР ДЛЯ РУССКОГО ЯЗЫКА ===" << std::endl;
    std::cout << "Корректная обработка UTF-8" << std::endl;
    std::cout << "==================================" << std::endl;

    if (argc == 2 && strcmp(argv[1], "--test") == 0) {
        RussianStemmer::test();
        return 0;
    }

    if (argc != 3) {
        std::cout << "\nИспользование:" << std::endl;
        std::cout << "  " << argv[0] << " <входная_папка> <выходная_папка>" << std::endl;
        std::cout << "  " << argv[0] << " --test  (тестирование)" << std::endl;
        std::cout << "\nПример:" << std::endl;
        std::cout << "  " << argv[0] << " tokens stems" << std::endl;
        return 1;
    }
    
    const char* input_dir = argv[1];
    const char* output_dir = argv[2];

    DWORD attribs = GetFileAttributes(input_dir);
    if (attribs == INVALID_FILE_ATTRIBUTES || !(attribs & FILE_ATTRIBUTE_DIRECTORY)) {
        std::cerr << "Ошибка: Входная папка не существует: " << input_dir << std::endl;
        return 1;
    }
    
    std::cout << "\nНачинаю обработку..." << std::endl;
    std::cout << "Входная папка:  " << input_dir << std::endl;
    std::cout << "Выходная папка: " << output_dir << std::endl;
    std::cout << "==================================" << std::endl;
    
    RussianStemmer::process_directory(input_dir, output_dir);
    
    std::cout << "==================================" << std::endl;
    std::cout << "Стемминг успешно завершен!" << std::endl;
    
    return 0;
}