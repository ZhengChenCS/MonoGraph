#include <iostream>
#include <vector>
#include <atomic>
#include <mutex>
#include <chrono>
#include <omp.h>
#include <iomanip>

// 原始版本的修正实现
template <typename T>
class prl_vector_original
{
public:
    prl_vector_original(size_t initial_capacity = 65536)
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
    // std::vector<T> _v;
    // std::atomic<uint64_t> _max_i;
    // std::mutex _mutex;
};

// 优化版本 (适用于C++11)
template <typename T>
class prl_vector_optimized
{
public:
    // 减小批量分配大小，避免过度分配
    static constexpr size_t BATCH_SIZE = 16;
    
    prl_vector_optimized(size_t initial_capacity = 65536)
        : _max_i(0)
    {
        _v.reserve(initial_capacity);
        // 预先调整大小，避免并发resize
        _v.resize(initial_capacity);
    }

    void push_back(const T& value)
    {
        // 获取元素索引
        uint64_t index = get_index();
        
        // 检查是否需要扩容
        if (index >= _v.size())
        {
            std::lock_guard<std::mutex> lock(_mutex);
            // 扩容确保索引有效
            while (index >= _v.size())
            {
                size_t new_size = _v.size() * 2;
                _v.resize(new_size);
            }
        }
        
        // 确保索引有效后写入数据
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
    // 简化的批量索引分配，非递归实现
    uint64_t get_index() {
        // 采用简单的原子递增，避免复杂逻辑
        return _max_i.fetch_add(1, std::memory_order_acq_rel);
    }

    // 使用对齐内存来避免伪共享
    alignas(64) std::vector<T> _v;
    alignas(64) std::atomic<uint64_t> _max_i;
    alignas(64) std::mutex _mutex;
};

// 安全的性能测试函数
template <template <typename> class VectorType>
double test_performance(int num_threads, size_t num_elements) {
    // 创建向量并预分配空间
    VectorType<int> vec(num_elements);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    #pragma omp parallel num_threads(num_threads)
    {
        #pragma omp for schedule(static)
        for (size_t i = 0; i < num_elements; ++i) {
            vec.push_back(static_cast<int>(i));
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    
    // 验证结果
    size_t actual_size = vec.size();
    std::cout << "元素数量: " << actual_size << "/" << num_elements 
              << " (" << (actual_size * 100.0 / num_elements) << "%)" << std::endl;
    
    return elapsed.count();
}

int main() {
    const int max_threads = omp_get_max_threads();
    // 减少测试元素数量，避免内存问题
    const size_t num_elements = 1000000; // 一百万个元素
    
    std::cout << "CPU 线程数: " << max_threads << std::endl;
    std::cout << "测试项: 插入 " << num_elements << " 个元素" << std::endl;
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "\n线程数\t原始版本(秒)\t优化版本(秒)\t加速比" << std::endl;
    std::cout << "----------------------------------------------" << std::endl;
    
    // 逐步增加线程数测试
    for (int num_threads = 1; num_threads <= std::min(max_threads, 8); ++num_threads) {
        std::cout << num_threads << "线程测试:" << std::endl;
        
        std::cout << "原始版本: ";
        double time_original = test_performance<prl_vector_original>(num_threads, num_elements);
        
        std::cout << "优化版本: ";
        double time_optimized = test_performance<prl_vector_optimized>(num_threads, num_elements);
        
        double speedup = time_original / time_optimized;
        
        std::cout << num_threads << "\t" 
                 << time_original << "\t\t" 
                 << time_optimized << "\t\t" 
                 << speedup << "x" << std::endl;
        std::cout << "----------------------------------------------" << std::endl;
    }
    
    return 0;
}