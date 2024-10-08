#include <stdio.h>
#include <stdlib.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>

#define RDMA_CHECK(cond, err_fmt, ...) \
    do { \
     if (!(cond)) { \
        fprintf(stderr, "%s:%d, %s, errno = %d, %s\n", __FILE__, __LINE__, err_fmt, \
            ##__VA_ARGS__, errno, strerror(errno));\
        exit(-1); \
     }\
    } while(0) \

const int IO_DEPTH = 32;
const int MAX_SGE = 30;

struct ibv_qp* 
create_qp(struct ibv_pd* pd, struct ibv_cq* cq) {
    struct ibv_qp_init_attr init_attr;
    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.send_cq = cq;
    init_attr.recv_cq = cq;
    init_attr.cap.max_send_wr = IO_DEPTH; // Number of working requests in send queue.
    init_attr.cap.max_recv_wr = IO_DEPTH; // Number of working requests in recv queue.
    init_attr.cap.max_send_sge = MAX_SGE; // Number of scatter/gather elements in the scatter/gather list.
    init_attr.cap.max_recv_sge = MAX_SGE; // Number of scatter/gather elements in the scatter/gather list.

    init_attr.qp_type = IBV_QPT_RC; // Queue pair type: Reliable Connection.
    struct ibv_qp *qp = ibv_create_qp(pd, &init_attr);
    RDMA_CHECK(qp, "ibv_create_qp() failed");
    return qp;
}

int init_qp(ibv_qp * qp){
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));

    // reset -> init
    qp_attr.qp_state = IBV_QPS_INIT;
    qp_attr.pkey_index = 0;
    qp_attr.port_num = 1; // PORT_NUM

    // See https://www.man7.org/linux/man-pages/man3/ibv_reg_mr.3.html for more details.
    qp_attr.qp_access_flags = (
        IBV_ACCESS_LOCAL_WRITE  | 
        IBV_ACCESS_REMOTE_WRITE | 
        IBV_ACCESS_REMOTE_READ
    );

    int ret = ibv_modify_qp(qp, &qp_attr, 
                            IBV_QP_STATE |
                            IBV_QP_PKEY_INDEX |
                            IBV_QP_PORT |
                            IBV_QP_ACCESS_FLAGS);
    RDMA_CHECK(ret == 0, "ibv_modify_qp() failed");
    return ret;
}

void print_qp_state(struct ibv_qp * qp) {
    struct ibv_qp_attr qp_attr;
    struct ibv_qp_init_attr init_attr;

    // attr_mask: see https://www.man7.org/linux/man-pages/man3/ibv_modify_qp.3.html for more details.
    int ret = ibv_query_qp(qp, &qp_attr, IBV_QP_STATE, &init_attr);
    RDMA_CHECK(ret == 0, "ibv_query_qp() error.");

    // enum ibv_qp_state {
    //     IBV_QPS_RESET,
    //     IBV_QPS_INIT,
    //     IBV_QPS_RTR,
    //     IBV_QPS_RTS,
    //     IBV_QPS_SQD,
    //     IBV_QPS_SQE,
    //     IBV_QPS_ERR,
    //     IBV_QPS_UNKNOWN
    // };
    printf("qp=%p, qp_state=%d\n", qp, qp_attr.qp_state);
}



int main() {
    int num_devices;
    struct ibv_device **device_list;
    struct ibv_context *context;
    struct ibv_pd *pd;
    struct ibv_comp_channel *channel;

    device_list = ibv_get_device_list(&num_devices);
    printf("Number of RDMA devices = %d\n", num_devices);

    // Open handle for device[0]
    context = ibv_open_device(device_list[0]);

    pd = ibv_alloc_pd(context);
    if (!pd) {
        fprintf(stderr, "Error: ibv_alloc_pd() failed !");
        return -1;
    }

    channel = ibv_create_comp_channel(context);
    if (!channel) {
        fprintf(stderr, "Error: ibv_create_comp_channel() failed ..");
        return -1;
    }

    struct ibv_cq *cq;
    cq = ibv_create_cq(context, 128, nullptr, channel, 0);

    union ibv_gid gid;
    int ret = ibv_query_gid(context, 1, 1, &gid);

    struct ibv_port_attr port_attr;
    ret = ibv_query_port(context, 1, &port_attr);

    struct ibv_qp *qp1;
    qp1 = create_qp(pd, cq);
    print_qp_state(qp1);

    // RESET -> INIT
    init_qp(qp1);
    print_qp_state(qp1);

    // INIT -> RTR (Ready to Receive)

    // RTR -> RTS (Ready to Send)




    ibv_destroy_cq(cq);
    ibv_destroy_comp_channel(channel);
    ibv_dealloc_pd(pd);
    ibv_close_device(context);
    ibv_free_device_list(device_list);
    return 0;
}