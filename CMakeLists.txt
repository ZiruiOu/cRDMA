cmake_minimum_required(VERSION 3.14)
project(RDMA_CodeBase)

add_executable(poll_cq src/poll_cq.cc)
target_link_libraries(
    poll_cq
    LINK_PRIVATE 
    rdmacm
    ibverbs
)