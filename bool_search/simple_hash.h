#ifndef SIMPLE_HASH_H
#define SIMPLE_HASH_H

#include <cstdlib>
#include <cstring>

class TermDict {
private:
    struct Node {
        char* key;
        int value;
        Node* next;
        
        Node(const char* k, int v) : value(v), next(nullptr) {
            key = static_cast<char*>(malloc(strlen(k) + 1));
            strcpy(key, k);
        }
        
        ~Node() {
            free(key);
        }
    };
    
    Node** buckets;
    size_t bucket_count;
    size_t size_;
    
    unsigned int hash(const char* str) const {
        unsigned int h = 5381;
        while (*str) {
            h = ((h << 5) + h) + *str++;
        }
        return h % bucket_count;
    }
    
public:
    TermDict(size_t init_size = 1024) : bucket_count(init_size), size_(0) {
        buckets = static_cast<Node**>(calloc(bucket_count, sizeof(Node*)));
    }
    
    ~TermDict() {
        clear();
        free(buckets);
    }
    
    void add(const char* key, int value) {
        unsigned int idx = hash(key);
        Node* node = buckets[idx];
        
        while (node) {
            if (strcmp(node->key, key) == 0) {
                node->value = value;
                return;
            }
            node = node->next;
        }
        
        Node* new_node = new Node(key, value);
        new_node->next = buckets[idx];
        buckets[idx] = new_node;
        size_++;
    }
    
    bool find(const char* key, int& result) const {
        unsigned int idx = hash(key);
        Node* node = buckets[idx];
        
        while (node) {
            if (strcmp(node->key, key) == 0) {
                result = node->value;
                return true;
            }
            node = node->next;
        }
        return false;
    }
    
    bool contains(const char* key) const {
        int dummy;
        return find(key, dummy);
    }
    
    void clear() {
        for (size_t i = 0; i < bucket_count; i++) {
            Node* node = buckets[i];
            while (node) {
                Node* next = node->next;
                delete node;
                node = next;
            }
            buckets[i] = nullptr;
        }
        size_ = 0;
    }
    
    size_t size() const { return size_; }
    
    class Iterator {
    private:
        TermDict* dict;
        size_t bucket_idx;
        Node* current;
        
        void next() {
            if (!current) return;
            if (current->next) {
                current = current->next;
                return;
            }
            
            bucket_idx++;
            while (bucket_idx < dict->bucket_count && !dict->buckets[bucket_idx]) {
                bucket_idx++;
            }
            current = (bucket_idx < dict->bucket_count) ? dict->buckets[bucket_idx] : nullptr;
        }
        
    public:
        Iterator(TermDict* d, size_t idx, Node* n) : dict(d), bucket_idx(idx), current(n) {}
        
        Iterator& operator++() {
            next();
            return *this;
        }
        
        bool operator!=(const Iterator& other) const {
            return current != other.current;
        }
        
        Node& operator*() { return *current; }
        Node* operator->() { return current; }
    };
    
    Iterator begin() {
        for (size_t i = 0; i < bucket_count; i++) {
            if (buckets[i]) {
                return Iterator(this, i, buckets[i]);
            }
        }
        return end();
    }
    
    Iterator end() {
        return Iterator(this, bucket_count, nullptr);
    }
};

#endif