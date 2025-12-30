#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <direct.h> 
#include <windows.h> 
#include "simple_vector.h"
#include "simple_hash.h"

struct DocEntry {
    int doc_id;
    SimpleVector<int> positions;
    
    DocEntry() : doc_id(0) {}
    DocEntry(int id) : doc_id(id) {}
    
    bool operator<(const DocEntry& other) const {
        return doc_id < other.doc_id;
    }
    
    bool operator>(const DocEntry& other) const {
        return doc_id > other.doc_id;
    }
};

struct TermData {
    SimpleVector<DocEntry> docs;
    int doc_count;
    
    TermData() : doc_count(0) {}
};

struct TermInfo {
    char term[256];
    int term_id;
    int doc_count;
    long file_offset;
    
    TermInfo() : term_id(0), doc_count(0), file_offset(0) {
        term[0] = '\0';
    }
    
    TermInfo(const char* t, int id) : term_id(id), doc_count(0), file_offset(0) {
        strncpy(term, t, sizeof(term) - 1);
        term[sizeof(term) - 1] = '\0';
    }
    
    bool operator<(const TermInfo& other) const {
        return strcmp(term, other.term) < 0;
    }
};

class BoolIndexer {
private:
    TermDict term_to_id;
    SimpleVector<TermData*> index_data;
    SimpleVector<char*> doc_names;
    int next_id;
    int doc_count;
    
    void ensure_capacity(int id) {
        while (static_cast<int>(index_data.size()) <= id) {
            index_data.push(nullptr);
        }
    }
    
public:
    BoolIndexer() : next_id(0), doc_count(0) {}
    
    ~BoolIndexer() {
        for (size_t i = 0; i < doc_names.size(); i++) {
            if (doc_names.get(i)) free(doc_names.get(i));
        }
        for (size_t i = 0; i < index_data.size(); i++) {
            if (index_data.get(i)) delete index_data.get(i);
        }
    }
    
    int add_doc(const char* name) {
        char* copy = static_cast<char*>(malloc(strlen(name) + 1));
        strcpy(copy, name);
        doc_names.push(copy);
        return doc_count++;
    }
    
    void add_occurrence(const char* term, int doc_id, int pos) {
        int term_id;
        if (!term_to_id.find(term, term_id)) {
            term_id = next_id++;
            term_to_id.add(term, term_id);
            ensure_capacity(term_id);
            index_data.get(term_id) = new TermData();
        }
        
        TermData* data = index_data.get(term_id);
        if (!data) return;
        
        DocEntry* entry = nullptr;
        for (size_t i = 0; i < data->docs.size(); i++) {
            if (data->docs.get(i).doc_id == doc_id) {
                entry = &data->docs.get(i);
                break;
            }
        }
        
        if (!entry) {
            DocEntry new_entry(doc_id);
            data->docs.push(new_entry);
            entry = &data->docs.get(data->docs.size() - 1);
            data->doc_count++;
        }
        
        entry->positions.push(pos);
    }
    
    void sort_all() {
        std::cerr << "Сортировка данных..." << std::endl;
        for (size_t i = 0; i < index_data.size(); i++) {
            if (index_data.get(i)) {
                index_data.get(i)->docs.sort_quick();
            }
        }
    }
    
    void save(const char* out_dir) {
        _mkdir(out_dir);
        
        SimpleVector<TermInfo> vocab;
        for (auto it = term_to_id.begin(); it != term_to_id.end(); ++it) {
            TermInfo info(it->key, it->value);
            if (it->value < static_cast<int>(index_data.size()) && index_data.get(it->value)) {
                info.doc_count = index_data.get(it->value)->doc_count;
            }
            vocab.push(info);
        }

        vocab.sort_quick();
        
        char vocab_path[512];
        char data_path[512];
        snprintf(vocab_path, sizeof(vocab_path), "%s/vocabulary.txt", out_dir);
        snprintf(data_path, sizeof(data_path), "%s/index_data.bin", out_dir);
        
        FILE* vocab_file = fopen(vocab_path, "w");
        FILE* data_file = fopen(data_path, "wb");
        
        if (!vocab_file || !data_file) {
            std::cerr << "Ошибка создания файлов" << std::endl;
            return;
        }
        
        long offset = 0;
        for (size_t i = 0; i < vocab.size(); i++) {
            TermInfo& info = vocab.get(i);
            TermData* data = index_data.get(info.term_id);
            if (!data) continue;
            
            fprintf(vocab_file, "%s\t%d\t%ld\n", info.term, info.doc_count, offset);
            
            int doc_count = data->docs.size();
            fwrite(&doc_count, sizeof(int), 1, data_file);
            
            for (int j = 0; j < doc_count; j++) {
                DocEntry& entry = data->docs.get(j);
                fwrite(&entry.doc_id, sizeof(int), 1, data_file);
                
                int pos_count = entry.positions.size();
                fwrite(&pos_count, sizeof(int), 1, data_file);
                for (int k = 0; k < pos_count; k++) {
                    fwrite(&entry.positions.get(k), sizeof(int), 1, data_file);
                }
            }
            
            offset = ftell(data_file);
        }
        
        fclose(vocab_file);
        fclose(data_file);
        
        char doclist_path[512];
        snprintf(doclist_path, sizeof(doclist_path), "%s/documents.txt", out_dir);
        FILE* doc_file = fopen(doclist_path, "w");
        if (doc_file) {
            for (size_t i = 0; i < doc_names.size(); i++) {
                fprintf(doc_file, "%zu\t%s\n", i, doc_names.get(i));
            }
            fclose(doc_file);
        }

        char stats_path[512];
        snprintf(stats_path, sizeof(stats_path), "%s/stats.txt", out_dir);
        FILE* stats_file = fopen(stats_path, "w");
        if (stats_file) {
            fprintf(stats_file, "Документов: %d\n", doc_count);
            fprintf(stats_file, "Уникальных терминов: %zu\n", term_to_id.size());
            fclose(stats_file);
        }
        
        std::cerr << "Индекс сохранён" << std::endl;
    }
    
    int doc_amount() const { return doc_count; }
    int term_amount() const { return term_to_id.size(); }
};

void process_file(const char* path, int doc_id, BoolIndexer& indexer) {
    FILE* file = fopen(path, "r");
    if (!file) {
        std::cerr << "Не могу открыть: " << path << std::endl;
        return;
    }
    
    char line[4096];
    while (fgets(line, sizeof(line), file)) {
        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') {
            line[len - 1] = '\0';
        }
        
        char* term = strtok(line, " \t");
        char* pos_str = strtok(nullptr, " \t");
        
        while (pos_str) {
            int pos = atoi(pos_str);
            if (pos > 0) {
                indexer.add_occurrence(term, doc_id, pos);
            }
            pos_str = strtok(nullptr, " \t");
        }
    }
    
    fclose(file);
}

void build_from_dir(const char* dir_path, const char* out_dir, BoolIndexer& indexer) {
    std::cerr << "Сканирую директорию: " << dir_path << std::endl;
    
    char search_path[MAX_PATH];
    snprintf(search_path, sizeof(search_path), "%s\\*.tokens", dir_path);
    
    WIN32_FIND_DATA find_data;
    HANDLE hFind = FindFirstFile(search_path, &find_data);
    
    if (hFind == INVALID_HANDLE_VALUE) {
        std::cerr << "Не найдены .tokens файлы" << std::endl;
        return;
    }
    
    SimpleVector<const char*> files;
    
    do {
        if (!(find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
            char* name_copy = static_cast<char*>(malloc(strlen(find_data.cFileName) + 1));
            strcpy(name_copy, find_data.cFileName);
            files.push(name_copy);
        }
    } while (FindNextFile(hFind, &find_data) != 0);
    
    FindClose(hFind);

    SimpleVector<int> file_nums;
    SimpleVector<const char*> sorted_files;
    
    for (size_t i = 0; i < files.size(); i++) {
        int doc_num;
        if (sscanf(files.get(i), "doc%d.tokens", &doc_num) == 1) {
            file_nums.push(doc_num);
        }
    }

    for (size_t i = 0; i < file_nums.size(); i++) {
        for (size_t j = i + 1; j < file_nums.size(); j++) {
            if (file_nums.get(i) > file_nums.get(j)) {
                int temp_num = file_nums.get(i);
                file_nums.get(i) = file_nums.get(j);
                file_nums.get(j) = temp_num;
                const char* temp_name = files.get(i);
                files.get(i) = files.get(j);
                files.get(j) = temp_name;
            }
        }
    }
    
    for (size_t i = 0; i < files.size(); i++) {
        char full_path[MAX_PATH];
        snprintf(full_path, sizeof(full_path), "%s\\%s", dir_path, files.get(i));
        
        int doc_num;
        if (sscanf(files.get(i), "doc%d.tokens", &doc_num) == 1) {
            int actual_id = indexer.add_doc(files.get(i));
            process_file(full_path, actual_id, indexer);
            
            if ((i + 1) % 100 == 0) {
                std::cerr << "Обработано " << (i + 1) << " файлов" << std::endl;
            }
        }
        
        free((void*)files.get(i));
    }
    
    std::cerr << "Всего: " << files.size() << " документов" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cout << "=== Булев индексатор (ЛР6) ===\n";
        std::cout << "Использование: " << argv[0] << " <папка_с_токенами> <выходная_папка>\n";
        std::cout << "Пример: " << argv[0] << " tokens index\n";
        std::cout << "Для работы нужна папка с .tokens файлами\n";
        return 1;
    }
    
    const char* input_dir = argv[1];
    const char* output_dir = argv[2];
    
    std::cerr << "=== Построение булева индекса ===\n";
    std::cerr << "Входная папка: " << input_dir << std::endl;
    std::cerr << "Выходная папка: " << output_dir << std::endl;
    
    BoolIndexer indexer;
    build_from_dir(input_dir, output_dir, indexer);
    
    std::cerr << "\nСортировка индекса..." << std::endl;
    indexer.sort_all();
    
    std::cerr << "Сохранение индекса..." << std::endl;
    indexer.save(output_dir);
    
    std::cerr << "\n=== Результаты ===\n";
    std::cerr << "Документов: " << indexer.doc_amount() << std::endl;
    std::cerr << "Уникальных терминов: " << indexer.term_amount() << std::endl;
    std::cerr << "Индекс сохранен в папке: " << output_dir << std::endl;
    
    return 0;
}