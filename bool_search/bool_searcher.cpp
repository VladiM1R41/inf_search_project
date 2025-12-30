#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include "simple_vector.h"
#include "simple_hash.h"

struct Posting {
    int doc_id;
    SimpleVector<int> poses;
    
    Posting() : doc_id(0) {}
    Posting(int id) : doc_id(id) {}
    
    bool operator<(const Posting& other) const {
        return doc_id < other.doc_id;
    }
    
    bool operator>(const Posting& other) const {
        return doc_id > other.doc_id;
    }
};

struct TermIndex {
    char term[256];
    int doc_count;
    long offset;
    
    TermIndex() : doc_count(0), offset(0) {
        term[0] = '\0';
    }
    
    bool operator<(const TermIndex& other) const {
        return strcmp(term, other.term) < 0;
    }
};

class SearchIndex {
private:
    SimpleVector<TermIndex> terms;
    SimpleVector<char*> doc_names;
    char data_path[512];
    int total_docs;
    
public:
    SearchIndex() : total_docs(0) {
        data_path[0] = '\0';
    }
    
    ~SearchIndex() {
        for (size_t i = 0; i < doc_names.size(); i++) {
            if (doc_names.get(i)) free(doc_names.get(i));
        }
    }
    
    bool load(const char* dir) {
        snprintf(data_path, sizeof(data_path), "%s/index_data.bin", dir);
        
        char vocab_path[512];
        snprintf(vocab_path, sizeof(vocab_path), "%s/vocabulary.txt", dir);
        
        FILE* vocab_file = fopen(vocab_path, "r");
        if (!vocab_file) {
            std::cerr << "Не найден файл vocabulary.txt" << std::endl;
            return false;
        }
        
        char line[1024];
        while (fgets(line, sizeof(line), vocab_file)) {
            TermIndex ti;
            if (sscanf(line, "%255[^\t]\t%d\t%ld", ti.term, &ti.doc_count, &ti.offset) == 3) {
                terms.push(ti);
            }
        }
        fclose(vocab_file);
        
        char docs_path[512];
        snprintf(docs_path, sizeof(docs_path), "%s/documents.txt", dir);
        FILE* docs_file = fopen(docs_path, "r");
        if (docs_file) {
            while (fgets(line, sizeof(line), docs_file)) {
                int id;
                char name[256];
                if (sscanf(line, "%d\t%255[^\n]", &id, name) == 2) {
                    while (static_cast<int>(doc_names.size()) <= id) {
                        doc_names.push(nullptr);
                    }
                    char* copy = static_cast<char*>(malloc(strlen(name) + 1));
                    strcpy(copy, name);
                    doc_names.get(id) = copy;
                    total_docs = id + 1;
                }
            }
            fclose(docs_file);
        }
        
        std::cerr << "Загружено: " << terms.size() << " терминов, "
                  << total_docs << " документов" << std::endl;
        return true;
    }
    
    SimpleVector<int> get_docs(const char* term) {
        SimpleVector<int> result;
        for (size_t i = 0; i < terms.size(); i++) {
            if (strcmp(terms.get(i).term, term) == 0) {
                FILE* file = fopen(data_path, "rb");
                if (!file) return result;
                
                fseek(file, terms.get(i).offset, SEEK_SET);
                
                int doc_count;
                fread(&doc_count, sizeof(int), 1, file);
                
                for (int j = 0; j < doc_count; j++) {
                    int doc_id, pos_count;
                    fread(&doc_id, sizeof(int), 1, file);
                    result.push(doc_id);
                    
                    fread(&pos_count, sizeof(int), 1, file);
                    fseek(file, pos_count * sizeof(int), SEEK_CUR);
                }
                
                fclose(file);
                break;
            }
        }
        
        return result;
    }
    
    const char* doc_name(int id) {
        if (id >= 0 && id < static_cast<int>(doc_names.size())) {
            return doc_names.get(id);
        }
        return nullptr;
    }
    
    int doc_total() const { return total_docs; }
};

SimpleVector<int> intersect(SimpleVector<int>& a, SimpleVector<int>& b) {
    SimpleVector<int> res;
    size_t i = 0, j = 0;

    a.sort();
    b.sort();
    
    while (i < a.size() && j < b.size()) {
        int d1 = a.get(i);
        int d2 = b.get(j);
        
        if (d1 == d2) {
            res.push(d1);
            i++; j++;
        } else if (d1 < d2) {
            i++;
        } else {
            j++;
        }
    }
    
    return res;
}

SimpleVector<int> unite(SimpleVector<int>& a, SimpleVector<int>& b) {
    SimpleVector<int> res;
    size_t i = 0, j = 0;
    
    a.sort();
    b.sort();
    
    while (i < a.size() && j < b.size()) {
        int d1 = a.get(i);
        int d2 = b.get(j);
        
        if (d1 == d2) {
            res.push(d1);
            i++; j++;
        } else if (d1 < d2) {
            res.push(d1);
            i++;
        } else {
            res.push(d2);
            j++;
        }
    }
    
    while (i < a.size()) {
        res.push(a.get(i));
        i++;
    }
    while (j < b.size()) {
        res.push(b.get(j));
        j++;
    }
    
    return res;
}

SimpleVector<int> complement(SimpleVector<int>& list, int total) {
    SimpleVector<int> res;
    int idx = 0;
    
    list.sort();
    
    for (int i = 0; i < total; i++) {
        if (idx < static_cast<int>(list.size()) && list.get(idx) == i) {
            idx++;
        } else {
            res.push(i);
        }
    }
    
    return res;
}

enum TokenType { WORD, AND, OR, NOT, LPAR, RPAR, END };

struct Token {
    TokenType type;
    char word[256];
    
    Token() : type(END) { word[0] = '\0'; }
    Token(TokenType t) : type(t) { word[0] = '\0'; }
    Token(const char* w) : type(WORD) {
        strncpy(word, w, sizeof(word) - 1);
        word[sizeof(word) - 1] = '\0';
    }
};

class QueryParser {
private:
    const char* input;
    int pos;
    Token current;
    
    void skip_spaces() {
        while (input[pos] && isspace(input[pos])) pos++;
    }
    
    void next_token() {
        skip_spaces();
        
        if (!input[pos]) {
            current = Token(END);
            return;
        }
        
        if (input[pos] == '(') {
            current = Token(LPAR);
            pos++;
            return;
        }
        
        if (input[pos] == ')') {
            current = Token(RPAR);
            pos++;
            return;
        }
        
        if (input[pos] == '&' && input[pos + 1] == '&') {
            current = Token(AND);
            pos += 2;
            return;
        }
        
        if (input[pos] == '|' && input[pos + 1] == '|') {
            current = Token(OR);
            pos += 2;
            return;
        }
        
        if (input[pos] == '!') {
            current = Token(NOT);
            pos++;
            return;
        }
        
        char buffer[256];
        int i = 0;
        while (input[pos] && !isspace(input[pos]) && 
               input[pos] != '(' && input[pos] != ')' &&
               input[pos] != '&' && input[pos] != '|' && input[pos] != '!') {
            if (i < 255) buffer[i++] = tolower(input[pos]);
            pos++;
        }
        buffer[i] = '\0';
        current = Token(buffer);
    }
    
    SimpleVector<int> parse_expr(SearchIndex& idx);
    SimpleVector<int> parse_term(SearchIndex& idx);
    SimpleVector<int> parse_factor(SearchIndex& idx);
    
public:
    QueryParser(const char* query) : input(query), pos(0) {
        next_token();
    }
    
    SimpleVector<int> parse(SearchIndex& idx) {
        return parse_expr(idx);
    }
};

SimpleVector<int> QueryParser::parse_expr(SearchIndex& idx) {
    SimpleVector<int> result = parse_term(idx);
    
    while (current.type == AND || current.type == OR) {
        TokenType op = current.type;
        next_token();
        SimpleVector<int> right = parse_term(idx);
        
        if (op == AND) {
            result = intersect(result, right);
        } else {
            result = unite(result, right);
        }
    }
    
    return result;
}

SimpleVector<int> QueryParser::parse_term(SearchIndex& idx) {
    if (current.type == NOT) {
        next_token();
        SimpleVector<int> inner = parse_factor(idx);
        return complement(inner, idx.doc_total());
    }
    
    return parse_factor(idx);
}

SimpleVector<int> QueryParser::parse_factor(SearchIndex& idx) {
    if (current.type == LPAR) {
        next_token();
        SimpleVector<int> result = parse_expr(idx);
        if (current.type != RPAR) {
            std::cerr << "Ошибка: ожидается ')'" << std::endl;
        } else {
            next_token();
        }
        return result;
    }
    
    if (current.type == WORD) {
        SimpleVector<int> result = idx.get_docs(current.word);
        next_token();
        return result;
    }
    
    return SimpleVector<int>();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "=== Булев поиск (ЛР7) ===\n";
        std::cout << "Использование: " << argv[0] << " <папка_с_индексом>\n";
        std::cout << "Пример: " << argv[0] << " index\n";
        std::cout << "Запросы читаются из stdin\n";
        std::cout << "Пример запроса: революция AND (франция OR париж) NOT война\n";
        return 1;
    }
    
    SearchIndex idx;
    if (!idx.load(argv[1])) {
        std::cerr << "Не удалось загрузить индекс" << std::endl;
        return 1;
    }
    
    std::cerr << "\n=== Булев поиск готов ===\n";
    std::cerr << "Введите запрос (или Ctrl+Z для выхода):\n> ";
    
    char query[4096];
    while (fgets(query, sizeof(query), stdin)) {
        size_t len = strlen(query);
        if (len > 0 && query[len - 1] == '\n') {
            query[len - 1] = '\0';
        }
        
        if (strlen(query) == 0) {
            std::cerr << "> ";
            continue;
        }
        
        std::cerr << "Запрос: " << query << std::endl;
        
        QueryParser parser(query);
        SimpleVector<int> results = parser.parse(idx);
        
        std::cout << "\nНайдено документов: " << results.size() << "\n";
        
        if (results.size() > 0) {
            std::cout << "Результаты:\n";
            for (size_t i = 0; i < results.size(); i++) {
                int doc_id = results.get(i);
                const char* name = idx.doc_name(doc_id);
                //std::cout << "  " << doc_id << "\t" << (name ? name : "?") << std::endl;
            }
        } else {
            std::cout << "По запросу ничего не найдено\n";
        }
        
        std::cerr << "\n> ";
    }
    
    return 0;
}