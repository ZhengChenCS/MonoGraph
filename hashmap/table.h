#ifndef TABLE_H
#define TABLE_H
#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <set>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
namespace py = pybind11;
class T_Graph;

class BasicTable
{
public:
    // 直接存储 Python DataFrame，避免拷贝
    explicit BasicTable(const py::object &basic_table)
        : table_name(py::str(basic_table.attr("name"))),
          col_names(basic_table.attr("header").cast<std::vector<std::string>>()),
          indexed_columns(basic_table.attr("indexed_columns").cast<std::set<std::string>>()),
          df(basic_table.attr("df"))
    { // 直接存 Python DataFrame
    }

    inline std::string getTableName() { return table_name; }
    inline std::vector<std::string> getColNames() { return col_names; }
    inline std::set<std::string> getIndexedColumns() { return indexed_columns; }
    // 获取 DataFrame 的行数
    size_t row_count() const
    {
        return py::len(df);
    }

    // 访问 DataFrame 中的单元格（无拷贝）
    py::object get_value(size_t row, const std::string &col_name) const
    {
        py::object row_obj = df.attr("iloc").attr("__getitem__")(row); // 访问指定行
        return row_obj.attr("__getitem__")(col_name);                  // 访问指定列
    }

    // 转换 DataFrame 为 NumPy 数组（按需转换）
    py::array get_numpy_array() const
    {
        return df.attr("values").cast<py::array>();
    }

    // 按列获取数据（避免拷贝）
    py::array get_column(const std::string &col_name) const
    {
        return df.attr("__getitem__")(col_name).attr("values").cast<py::array>();
    }

    py::object get_index_value(size_t cur) const
    {
        py::object index_obj = df.attr("index");   // 获取 DataFrame 的 index 属性
        return index_obj.attr("__getitem__")(cur); // 获取第 cur 个索引标签
    }

    friend class T_Graph;

private:
    std::string table_name;
    std::vector<std::string> col_names;
    std::set<std::string> indexed_columns;
    py::object df; // 直接存 Python 的 DataFrame
};

class VertexTable : public BasicTable
{
public:
    explicit VertexTable(const py::object &vertex_table) : BasicTable(vertex_table)
    {
    }
};

PYBIND11_MODULE(example, m)
{
    py::class_<VertexTable>(m, "VertexTable")
        .def(py::init<const py::object &>());
};

class EdgeTable : public BasicTable
{
public:
    explicit EdgeTable(const py::object &edge_table) : BasicTable(edge_table),
                                                       src_column_name(edge_table.attr("src_column_name").cast<std::string>()),
                                                       dst_column_name(edge_table.attr("dst_column_name").cast<std::string>())
    {
    }
    std::string src_column_name;
    std::string dst_column_name;
};
#endif