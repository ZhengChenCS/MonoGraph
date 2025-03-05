#include <iostream>
#include <vector>
#include <atomic>
#include <mutex>
#include <chrono>
#include <omp.h>
#include <iomanip>
template <typename T>
class prl_vector
{
public:
    prl_vector(size_t initial_capacity = 65536)
        : _max_i(0)
    {
        _v.reserve(initial_capacity);
        // 预先调整大小，避免并发resize
        _v.resize(initial_capacity);
    }

    void push_back(const T& value)
    {
        uint64_t index = _max_i.fetch_add(1, std::memory_order_acq_rel);
        
        if (index >= _v.size())
        {
            std::lock_guard<std::mutex> lock(_mutex);
            // 确保容量足够
            while (index >= _v.size())
            {
                size_t new_size = _v.size() * 2;
                _v.resize(new_size);
            }
        }
        
        // 确保索引有效
        if (index < _v.size()) {
            _v[index] = value;
        }
    }

    auto begin() { return _v.begin(); }
    auto end() { return _v.begin() + _max_i.load(std::memory_order_acquire); }
    
    T& operator[](size_t index) { return _v[index]; }
    const T& operator[](size_t index) const { return _v[index]; }
    
    size_t size() const { return _max_i.load(std::memory_order_acquire); }

private:
    // 使用对齐内存来避免伪共享
    alignas(64) std::vector<T> _v;
    alignas(64) std::atomic<uint64_t> _max_i;
    alignas(64) std::mutex _mutex;
};
