#ifndef T_GRAPH_H
#define T_GRAPH_H

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <iostream>
#include <libcuckoo/cuckoohash_map.hh>
#include <mutex>
#include <omp.h>
#include <shared_mutex>
#include <thread>
#include <cstdint>
#include <vector>
#include <string>
#include <atomic>
#include <fstream>
#include "prl_vector.h"
#include "table.h"

namespace py = pybind11;

class T_Graph
{
public:
    T_Graph(const std::string &name);
    
    void saveGraph(const std::string &path);
    void saveIdMap(const std::string &path);
    
    void transformVertexTable(const py::object &vertex_table);
    void transformEdgeTable(const py::object &edge_table);
    
    void print(){std::cout << "hhh" << std::endl;};

private:
    uint64_t createVertex(const std::string &key); // return _id_map[key]
    uint64_t getId(const std::string &key);
    void createEdge(const std::string &src, const std::string &dst);
    void createEdgeMapping(const uint64_t &src, const uint64_t &dst);
    inline std::string table_encoding(const std::string &key)
    {
        return "#" + key;
    }
    inline std::string column_encoding(const std::string &key, const uint64_t &table_id)
    {
        return "#" + key + "_" + std::to_string(table_id);
    }
    inline std::string edge_id_encoding(const std::string &key, const uint64_t &table_id)
    {
        return std::to_string(table_id) + "_" + key;
    }
    inline std::string primary_key_encoding(const std::string &key, const uint64_t &table_id)
    {
        return std::to_string(table_id) + "_" + key;
    }
    std::string _name;
    std::atomic<uint64_t> _max_id;
    prl_vector<std::pair<uint64_t, uint64_t>> _edge;
    libcuckoo::cuckoohash_map<std::string, uint64_t> _id_map; // vertex id_map
    std::atomic<uint64_t> _vertex_cnt;
    std::atomic<uint64_t> _edge_cnt;
};

#endif