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
#include <unordered_map>
namespace py = pybind11;
class T_Graph;

struct LocalColumn {
    std::vector<int64_t> int_data;
    std::vector<double> float_data;
    std::vector<std::string> string_data;
    enum { INT, FLOAT, STRING } dtype;

    std::string get_value(size_t index) const {
        switch (dtype) {
            case INT: return std::to_string(int_data[index]);
            case FLOAT: return std::to_string(float_data[index]);
            case STRING: return string_data[index];
            default: throw std::runtime_error("Unknown dtype");
        }
    }
};

class BasicTable
{
public:
    std::unordered_map<std::string, LocalColumn> local_columns; // 普通列
    LocalColumn local_index;                                    
    // 直接存储 Python DataFrame，避免拷贝
    explicit BasicTable(const py::object &basic_table)
        : table_name(py::str(basic_table.attr("name"))),
          col_names(basic_table.attr("header").cast<std::vector<std::string>>()),
          indexed_columns(basic_table.attr("indexed_columns").cast<std::set<std::string>>()),
          df(basic_table.attr("df"))
    { // 直接存 Python DataFrame

        if (!df.attr("index").is_none()) {
            py::array index_array = get_index_array();
            copy_array_to_local(index_array, local_index);
        }

        // 处理其他列
        for (const auto &col_name : col_names) {
            if (indexed_columns.count(col_name)) continue; // 索引列已处理
            py::array col_array = get_column(col_name);
            copy_array_to_local(col_array, local_columns[col_name]);
        }
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
    py::array get_column(const std::string &col_name)
    {
        return df.attr("__getitem__")(col_name).attr("values").cast<py::array>();
    }

    py::array get_index_array() {
        // 获取 DataFrame 的 index 属性并转换为 NumPy 数组
        return df.attr("index").attr("values").cast<py::array>();
    }

    py::object get_index_value(size_t cur) const
    {
        py::gil_scoped_acquire acquire;
        py::object index_obj = df.attr("index");   // 获取 DataFrame 的 index 属性
        return index_obj.attr("__getitem__")(cur); // 获取第 cur 个索引标签
    }

    friend class T_Graph;

private:
    std::string table_name;
    std::vector<std::string> col_names;
    std::set<std::string> indexed_columns;
    py::object df; // 直接存 Python 的 DataFrame

    void copy_array_to_local(py::array &src, LocalColumn &dest) {
        auto buf = src.request();
        char dtype = src.dtype().kind();
        if (dtype == 'i' || dtype == 'l') {
            dest.dtype = LocalColumn::INT;
            auto *ptr = static_cast<int64_t*>(buf.ptr);
            dest.int_data.assign(ptr, ptr + buf.size);
        } else if (dtype == 'f' || dtype == 'd') {
            dest.dtype = LocalColumn::FLOAT;
            auto *ptr = static_cast<double*>(buf.ptr);
            dest.float_data.assign(ptr, ptr + buf.size);
        } else if (dtype == 'O') {
            dest.dtype = LocalColumn::STRING;
            dest.string_data.reserve(buf.size);
            PyObject** obj_ptr = static_cast<PyObject**>(buf.ptr);
            for (size_t i = 0; i < buf.size; ++i) {
                py::handle py_obj(obj_ptr[i]); // 管理引用计数
                if (py::isinstance<py::str>(py_obj)) {
                    std::string str = py_obj.cast<std::string>();
                    dest.string_data.push_back(str);
                }
            }
        } else {
            throw std::runtime_error("Unsupported dtype: " + dtype);
        }
    }
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