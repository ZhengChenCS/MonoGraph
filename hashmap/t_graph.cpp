#include "t_graph.h"
std::string to_string(py::object obj) {
    // 如果 obj 是字符串
    if (py::isinstance<py::str>(obj)) {
        return obj.cast<std::string>();
    }
    // 如果 obj 是整数
    else if (py::isinstance<py::int_>(obj)) {
        return std::to_string(obj.cast<int>());
    }
    // 如果 obj 是浮点数
    else if (py::isinstance<py::float_>(obj)) {
        return std::to_string(obj.cast<double>());
    }
    // 如果 obj 不是预期的类型，可以处理错误或返回一个默认值
    else {
        return "unknown_type";
    }
}
T_Graph::T_Graph(const std::string &name) : _name(name), _max_id(0), _vertex_cnt(0), _edge_cnt(0) {};
uint64_t T_Graph::createVertex(const std::string &key)
{
    uint64_t id = 0;
    _id_map.upsert(
        key,
        [&](uint64_t &existingID)
        { id = existingID; },                               // key 存在时更新
        _max_id.fetch_add(1, std::memory_order_acq_rel) + 1 // key 不存在时插入的值
    );
    return id;
}

uint64_t T_Graph::getId(const std::string &key)
{
    if (!_id_map.contains(key))
    {
        std::cerr << key << " not exist in id map.\n";
        throw std::runtime_error("Key not found in id_map");
        // return (uint64_t)(-1);
    }
    return _id_map.find(key);
};
void T_Graph::createEdge(const std::string &src, const std::string &dst)
{
    _edge.push_back(std::make_pair(getId(src), getId(dst)));
    _edge_cnt.fetch_add(1, std::memory_order_acq_rel);
};
void T_Graph::createEdgeMapping(const uint64_t &src, const uint64_t &dst)
{
    _edge.push_back(std::make_pair(src, dst));
    _edge_cnt.fetch_add(1, std::memory_order_acq_rel);
};
void T_Graph::saveGraph(const std::string &path)
{
    std::ofstream file(path);
    if (!file)
    {
        std::cerr << "Error: Unable to open file " << path << std::endl;
        return;
    }
    for (const auto &pair : _edge)
    {
        file << pair.first << " " << pair.second << "\n";
    }
    file.close();
};
void T_Graph::saveIdMap(const std::string &path)
{
    std::ofstream file(path, std::ios::binary);
    if (!file)
    {
        std::cerr << "Error: Unable to open file " << path << std::endl;
        return;
    }
    size_t size = _id_map.size();
    file.write(reinterpret_cast<const char *>(&size), sizeof(size));
    auto lt = _id_map.lock_table();
    for (const auto &pair : lt)
    {
        file.write(reinterpret_cast<const char *>(&pair.first), sizeof(pair.first));
        file.write(reinterpret_cast<const char *>(&pair.second), sizeof(pair.second));
    }
    file.close();
    std::cout << "id map has saved into " << path << std::endl;
};

void T_Graph::transformVertexTable(const py::object &vertex_table)
{
    VertexTable vt(vertex_table);
    createVertex(table_encoding(vt.getTableName()));
    auto table_id = getId(table_encoding(vt.getTableName()));

    // 创建每一个列
    auto num_rows = vt.row_count();
    auto col_names = vt.getColNames();

    for (auto &col_name : vt.getColNames())
    {
        createVertex(column_encoding(col_name, table_id));
        auto col_id = getId(column_encoding(col_name, table_id));
        createEdgeMapping(table_id, col_id);
    }

    auto primary_index = vt.indexed_columns.begin();

#pragma omp parallel for
    for (auto cur = 0; cur < num_rows; cur++)
    {
        auto primary_id = -1;
        for (auto const &col_name : col_names)
        {
            auto col_id = getId(column_encoding(col_name, table_id));
            if (col_name == *primary_index)
            {
                auto data = to_string(vt.get_index_value(cur));
                createVertex(primary_key_encoding(data, table_id));
                auto primary_id = getId(primary_key_encoding(data, table_id));
                createEdgeMapping(primary_id, col_id);
            }
            else
            {
                auto data = to_string(vt.get_value(cur, col_name));
                createVertex(data);
                auto value_id = getId(data);
                createEdgeMapping(value_id, col_id);
                createEdgeMapping(value_id, primary_id);
            }
        }
    }

    // int num_rows = df.begin()->second.size();
}

void T_Graph::transformEdgeTable(const py::object &edge_table)
{
    EdgeTable et(edge_table);
    auto col_names = et.getColNames();
    auto table_name = et.getTableName();
    createVertex(table_encoding(table_name));
    auto table_id = getId(table_encoding(table_name));

    auto num_rows = et.row_count();
    auto num_cols = col_names.size();
    createVertex(table_encoding(et.src_column_name));
    createVertex(table_encoding(et.dst_column_name));
    auto src_table_id = getId(table_encoding(et.src_column_name));
    auto dst_table_id = getId(table_encoding(et.dst_column_name));

    libcuckoo::cuckoohash_map<std::string, uint64_t> colName2Id;

    for (size_t i = 0; i < num_cols; i++)
    {
        createVertex(column_encoding(col_names[i], table_id));
        auto col_name_id = getId(column_encoding(col_names[i], table_id));
        colName2Id.insert(col_names[i], col_name_id);
        createEdgeMapping(col_name_id, table_id);
    }

    for (size_t i = 0; i < num_rows; i++)
    {
        createVertex(edge_id_encoding(std::to_string(i), table_id));
        auto edge_id = getId(edge_id_encoding(std::to_string(i), table_id));
        createEdgeMapping(edge_id, table_id);
    }

    // create edge prop value (not including src and dst)
#pragma omp parallel for
    for (size_t cur = 0; cur < num_rows; cur++)
    {
        for (auto const &col_name : col_names)
        {
            if (col_name == et.src_column_name || col_name == et.dst_column_name)
            {
                continue;
            }
            else
            {
                auto data = to_string(et.get_value(cur, col_name));
                createVertex(data);
                auto value_id = getId(data);
                createEdgeMapping(colName2Id.find(col_name), value_id);
                createEdge(edge_id_encoding(std::to_string(cur), table_id), data);
            }
        }
    }

    // create edge from src to dst
#pragma omp parallel for
    for (size_t i = 0; i < num_rows; i++)
    {
        auto edge_id = getId(edge_id_encoding(std::to_string(i), table_id));
        auto src_value = to_string(et.get_value(i, et.src_column_name));
        auto dst_value = to_string(et.get_value(i, et.dst_column_name));
        createVertex(primary_key_encoding(src_value, src_table_id));
        createVertex(primary_key_encoding(dst_value, dst_table_id));
        auto src_id = getId(primary_key_encoding(src_value, src_table_id));
        auto dst_id = getId(primary_key_encoding(dst_value, dst_table_id));
        createEdgeMapping(edge_id, src_id);
        createEdgeMapping(edge_id, dst_id);
    }
}

PYBIND11_MODULE(graph_bind, m)
{
    // 绑定 BasicTable
    py::class_<BasicTable>(m, "BasicTable")
        .def(py::init<const py::object &>())
        .def("row_count", &BasicTable::row_count)
        .def("get_value", &BasicTable::get_value)
        .def("get_numpy_array", &BasicTable::get_numpy_array)
        .def("get_column", &BasicTable::get_column)
        .def("getTableName", &BasicTable::getTableName)
        .def("getColNames", &BasicTable::getColNames)
        .def("getIndexedColumns", &BasicTable::getIndexedColumns)
        .def("get_index_value", &BasicTable::get_index_value);

    // 绑定 VertexTable 继承 BasicTable
    py::class_<VertexTable, BasicTable>(m, "VertexTable")
        .def(py::init<const py::object &>());

    // 绑定 EdgeTable 继承 BasicTable，并添加 src_column_name 和 dst_column_name
    py::class_<EdgeTable, BasicTable>(m, "EdgeTable")
        .def(py::init<const py::object &>())
        .def_readonly("src_column_name", &EdgeTable::src_column_name)
        .def_readonly("dst_column_name", &EdgeTable::dst_column_name);

    py::class_<T_Graph>(m, "T_Graph")
        .def(py::init<const std::string &>())                        // 绑定构造函数
        .def("saveGraph", &T_Graph::saveGraph)                       // 绑定 saveGraph
        .def("saveIdMap", &T_Graph::saveIdMap)                       // 绑定 saveIdMap
        .def("transformVertexTable", &T_Graph::transformVertexTable) // 绑定 transformVertexTable
        .def("transformEdgeTable", &T_Graph::transformEdgeTable)     // 绑定 transformEdgeTable
        .def("print", &T_Graph::print);
}
