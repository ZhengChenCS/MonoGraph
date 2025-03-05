g++ -shared -std=c++17 -fPIC \
    `python3 -m pybind11 --includes` \
    t_graph.cpp \
    $(find libcuckoo -name "*.cpp") \
    prl_vector.h \
    table.h \
    -o graph_bind`python3-config --extension-suffix` \
    -I/usr/include/python3.x

