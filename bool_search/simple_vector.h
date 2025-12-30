#ifndef SIMPLE_VECTOR_H
#define SIMPLE_VECTOR_H

#include <cstdlib>
#include <cstring>
#include <new>

template<typename T>
class SimpleVector {
private:
    T* items;
    size_t count;
    size_t capacity_;
    
    void grow() {
        size_t new_cap = capacity_ ? capacity_ * 2 : 4;
        T* new_items = static_cast<T*>(malloc(new_cap * sizeof(T)));
        for (size_t i = 0; i < count; i++) {
            new (&new_items[i]) T(items[i]);
            items[i].~T();
        }
        free(items);
        items = new_items;
        capacity_ = new_cap;
    }
    
public:
    SimpleVector() : items(nullptr), count(0), capacity_(0) {}
    
    ~SimpleVector() {
        clear();
        free(items);
    }
    
    SimpleVector(const SimpleVector& other) : items(nullptr), count(0), capacity_(0) {
        if (other.count > 0) {
            items = static_cast<T*>(malloc(other.capacity_ * sizeof(T)));
            capacity_ = other.capacity_;
            for (size_t i = 0; i < other.count; i++) {
                new (&items[i]) T(other.items[i]);
                count++;
            }
        }
    }
    
    SimpleVector& operator=(const SimpleVector& other) {
        if (this != &other) {
            clear();
            free(items);
            items = nullptr;
            count = capacity_ = 0;
            
            if (other.count > 0) {
                items = static_cast<T*>(malloc(other.capacity_ * sizeof(T)));
                capacity_ = other.capacity_;
                for (size_t i = 0; i < other.count; i++) {
                    new (&items[i]) T(other.items[i]);
                    count++;
                }
            }
        }
        return *this;
    }
    
    void push(const T& val) {
        if (count >= capacity_) grow();
        new (&items[count]) T(val);
        count++;
    }
    
    void pop() {
        if (count > 0) {
            count--;
            items[count].~T();
        }
    }
    
    T& get(size_t idx) { return items[idx]; }
    const T& get(size_t idx) const { return items[idx]; }
    
    size_t size() const { return count; }
    size_t capacity() const { return capacity_; }
    bool empty() const { return count == 0; }
    
    void clear() {
        for (size_t i = 0; i < count; i++) {
            items[i].~T();
        }
        count = 0;
    }
    
    void reserve(size_t new_cap) {
        if (new_cap <= capacity_) return;
        T* new_items = static_cast<T*>(malloc(new_cap * sizeof(T)));
        for (size_t i = 0; i < count; i++) {
            new (&new_items[i]) T(items[i]);
            items[i].~T();
        }
        free(items);
        items = new_items;
        capacity_ = new_cap;
    }
    
    void sort() {
        if (count <= 1) return;
        for (size_t i = 1; i < count; i++) {
            T key = items[i];
            int j = i - 1;
            while (j >= 0 && key < items[j]) {
                items[j + 1] = items[j];
                j--;
            }
            items[j + 1] = key;
        }
    }
    
    void quick_sort(int left, int right) {
        if (left >= right) return;
        
        T pivot = items[(left + right) / 2];
        int i = left, j = right;
        
        while (i <= j) {
            while (items[i] < pivot) i++;
            while (pivot < items[j]) j--;
            
            if (i <= j) {
                T temp = items[i];
                items[i] = items[j];
                items[j] = temp;
                i++;
                j--;
            }
        }
        
        if (left < j) quick_sort(left, j);
        if (i < right) quick_sort(i, right);
    }
    
    void sort_quick() {
        if (count > 1) {
            quick_sort(0, count - 1);
        }
    }
};

#endif