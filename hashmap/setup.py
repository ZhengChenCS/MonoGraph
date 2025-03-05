from setuptools import setup, Extension
import pybind11

setup(
    name='graph_bind',
    ext_modules=[
        Extension(
            'graph_bind',  # 模块名称
            ['t_graph.cpp'],  # C++ 源代码文件
            include_dirs=[pybind11.get_include()],  # pybind11 头文件路径
            extra_compile_args=['-fopenmp'],  # 启用 OpenMP 支持
            extra_link_args=['-fopenmp'],  # 链接时添加 OpenMP
            language='c++',  # 使用 C++ 编译
        ),
    ],
)
