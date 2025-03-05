from setuptools import setup, Extension
import glob
import pybind11

# 获取 csrc 目录下的所有 .cpp 文件
cpp_files = glob.glob('csrc/*.cpp')
print(cpp_files)

# 定义扩展模块
libmono_extension = Extension(
    name='libmono',  # 模块名称
    sources=cpp_files,  # 源文件列表
    include_dirs=[
        pybind11.get_include(),  # 添加 pybind11 的 include 目录
        'third_party/'    # 添加 third_party 中的头文件目录
    ],
    extra_compile_args=['-std=c++17', '-fopenmp', '-Ofast'],  # 添加编译选项
    extra_link_args=['-fopenmp'],
)

setup(
    name='libmono',
    version='0.1',
    description='A C++ library for MonoGraph with pybind11',
    ext_modules=[libmono_extension],
    install_requires=['pybind11>=2.6.0'],  # 确保安装 pybind11
)
