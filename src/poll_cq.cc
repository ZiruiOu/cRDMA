#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#define RDMA_CHECK(cond, err_fmt, ...) \
    do { \
     if (!(cond)) { \
        fprintf(stderr, "%s:%d, " err_fmt ", errno = %d, %s\n", __FILE__, __LINE__, \
            ##__VA_ARGS__, errno, strerror(errno));\
        exit(-1); \
     }\
    } while(0) \

const int IO_DEPTH = 32;
const int MAX_SGE = 30;
const int PORT_NUM = 1;

typedef struct rdma_dest {
    uint16_t lid;
    uint32_t qpn;
    uint32_t psn;
    union ibv_gid gid;
} rdma_dest_t;

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

int modify_rtr_state(ibv_qp* qp, rdma_dest_t* dest) {
    struct ibv_qp_attr qp_attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_4096,
        .rq_psn = dest->psn,
        .dest_qp_num = dest->qpn,
        .ah_attr = {
            .dlid = dest->lid,
            .sl = 0,
            .src_path_bits = 0,
            .is_global = 0,
            .port_num = 1
        },
        .max_dest_rd_atomic = 1,
        .min_rnr_timer = 12,
    };

    //qp_attr.ah_attr.grh.sgid_index = 1;
    //qp_attr.ah_attr.grh.hop_limit = 1;
    //qp_attr.ah_attr.grh.dgid = gid;

    int ret = ibv_modify_qp(qp, &qp_attr, 
                            IBV_QP_STATE              |
                            IBV_QP_AV                 |
                            IBV_QP_PATH_MTU           | 
                            IBV_QP_DEST_QPN           | 
                            IBV_QP_RQ_PSN             | 
                            IBV_QP_MAX_DEST_RD_ATOMIC | 
                            IBV_QP_MIN_RNR_TIMER);
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

int pp_connect(ibv_qp *qp, const rdma_dest_t *rem_dest, const rdma_dest_t *my_dest) {
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_4096,
        .rq_psn = rem_dest->psn,
        .dest_qp_num = rem_dest->qpn,
        .ah_attr = {
            .dlid = rem_dest->lid,
            .sl = 0,
            .src_path_bits = 0,
            .is_global = 0,
            .port_num = 1
        },
        .max_dest_rd_atomic = 1,
        .min_rnr_timer = 12,
    };

    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.grh.dgid = rem_dest->gid;
    attr.ah_attr.grh.sgid_index = 1;

    int rc = ibv_modify_qp(qp, &attr, 
                            IBV_QP_STATE              |
                            IBV_QP_AV                 |
                            IBV_QP_PATH_MTU           | 
                            IBV_QP_DEST_QPN           | 
                            IBV_QP_RQ_PSN             | 
                            IBV_QP_MAX_DEST_RD_ATOMIC | 
                            IBV_QP_MIN_RNR_TIMER);

    if (rc < 0) {
        fprintf(stderr, "rdma_connect: ibv_modify_qp fail to convert INIT->RTR ...");
        return rc;
    }

    print_qp_state(qp);

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = my_dest->psn;
    attr.max_rd_atomic = 1;

    rc = ibv_modify_qp(qp, &attr, 
                       IBV_QP_STATE                 |
                       IBV_QP_TIMEOUT               |
                       IBV_QP_RETRY_CNT             | 
                       IBV_QP_RNR_RETRY             |
                       IBV_QP_SQ_PSN                |
                       IBV_QP_MAX_DEST_RD_ATOMIC    |
                       IBV_QP_MAX_QP_RD_ATOMIC);
    if (rc < 0) {
        fprintf(stderr, "rdma_connect: ibv_modify_qp fail to convert RTR -> RTS");
        return rc;
    }

    return 0;
}


//const char server_addr[] = "172.16.112.43";
const char server_name[] = "172.16.112.43";
const int port = 7788;

void serialize_gid(const union ibv_gid* gid, char *buffer) {
    // __be64 __be64
    sprintf(buffer, "%016lx:%016lx", be64toh(gid->global.subnet_prefix), be64toh(gid->global.interface_id));
}

void deserialize_gid(const char *buffer, union ibv_gid* gid) {
    uint64_t a, b;
    sscanf(buffer, "%lx:%lx", &a, &b);
    gid->global.subnet_prefix = htobe64(a);
    gid->global.interface_id = htobe64(b);
}


rdma_dest_t*
server_exchange_dest(const rdma_dest_t* my_dest) {
    struct addrinfo *res;
    struct addrinfo hints = {
        .ai_flags = AI_PASSIVE,
        .ai_family = AF_UNSPEC,
        .ai_socktype = SOCK_STREAM,
    };
    int sockfd, connfd;
    char service[5];
    memset(service, 0, sizeof(service));
    sprintf(service, "%d", port);

    int n = getaddrinfo(server_name, service, &hints, &res);
    RDMA_CHECK(n >= 0, "getaddrinfo error ");

    for(struct addrinfo *t = res; t; t = t->ai_next) {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 1) {
            n = 1;

            // TODO: set SO_REUSEADDR ?
            if (!bind(sockfd, t->ai_addr, t->ai_addrlen)) {
                break;
            }
            close(sockfd);
            sockfd = -1;
        }
    }
    freeaddrinfo(res);

    RDMA_CHECK(sockfd >= 0, "Couldn't listen on port %d", port);

    printf("Listening on socket fd = %d\n", sockfd);
    listen(sockfd, 1);
    connfd = accept(sockfd, NULL, NULL);
    close(sockfd);
    RDMA_CHECK(connfd >= 0, "accept() failed.");

    char gid_buf[34];
    char msg[sizeof "0000:000000:000000:0000000000000000:0000000000000000"];
    memset(gid_buf, 0, sizeof(gid_buf));
    memset(msg, 0, sizeof(msg));

    rdma_dest_t *rem_dest = NULL;
    // receive msg from client.
    n = read(connfd, msg, sizeof(msg));
    if (n != sizeof(msg)) {
        fprintf(stderr, "server_exchange_dest: cannot read remote address.");
        goto on_fail;
    }

    // deserialize from msg
    rem_dest = (rdma_dest_t*) malloc(sizeof(rdma_dest_t));
    sscanf(msg, "%hx:%x:%x:%s", &rem_dest->lid, &rem_dest->psn, &rem_dest->qpn, gid_buf);
    deserialize_gid(gid_buf, &rem_dest->gid);

    memset(msg, 0, sizeof(msg));
    serialize_gid(&my_dest->gid, gid_buf);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->psn, my_dest->qpn, gid_buf);

    n = write(connfd, msg, sizeof(msg));
    if (n != sizeof(msg)) {
        fprintf(stderr, "Coundn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto on_fail;
    }

    n = read(connfd, msg, sizeof(msg));
    if (n != sizeof "DONE") {
        fprintf(stderr, "Coundn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto on_fail;
    }

on_fail:
    close(connfd);
    return rem_dest;
}

rdma_dest_t*
client_exchange_dest(const rdma_dest_t* my_dest) {
    struct addrinfo *res;
    struct addrinfo hints = {
        .ai_family = AF_UNSPEC,
        .ai_socktype = SOCK_STREAM,
    };

    int sockfd;
    char service[5];
    memset(service, 0, sizeof(service));
    sprintf(service, "%d", port);

    int n = getaddrinfo(server_name, service, &hints, &res);
    RDMA_CHECK(n >= 0, "getaddrinfo error ");

    for(struct addrinfo *t = res; t; t = t->ai_next) {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 1) {
            n = 1;

            if (!connect(sockfd, t->ai_addr, t->ai_addrlen)) {
                break;
            }
            close(sockfd);
            sockfd = -1;
        }
    }
    freeaddrinfo(res);

    RDMA_CHECK(sockfd >= 0, "Couldn't connect to %s:%d", server_name, port);


    char gid_buf[34];
    char msg[sizeof "0000:000000:000000:0000000000000000:0000000000000000"];
    memset(gid_buf, 0, sizeof(gid_buf));
    memset(msg, 0, sizeof(msg));

    rdma_dest_t *rem_dest = NULL;

    // send local address to server ...
    memset(msg, 0, sizeof(msg));
    serialize_gid(&my_dest->gid, gid_buf);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->psn, my_dest->qpn, gid_buf);
    n = write(sockfd, msg, sizeof(msg));
    if (n != sizeof(msg)) {
        fprintf(stderr, "client_exchange_dest: cannot send local address.");
        goto on_fail;
    }

    n = read(sockfd, msg, sizeof(msg));
    if (n != sizeof(msg)) {
        fprintf(stderr, "Coundn't send local address\n");
        goto on_fail;
    }

    // deserialize from msg
    rem_dest = (rdma_dest_t*) malloc(sizeof(rdma_dest_t));
    sscanf(msg, "%hx:%x:%x:%s", &rem_dest->lid, &rem_dest->psn, &rem_dest->qpn, gid_buf);
    deserialize_gid(gid_buf, &rem_dest->gid);

    n = write(sockfd, "DONE", sizeof "DONE");
    if (n != sizeof "DONE") {
        fprintf(stderr, "Coundn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto on_fail;
    }

on_fail:
    close(sockfd);
    return rem_dest;
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
    printf("Opening device : %s\n", device_list[0]->name);

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

    struct ibv_port_attr port_attr;
    int ret = ibv_query_port(context, PORT_NUM, &port_attr);
    printf("Device %s has %d active gidx\n", context->device->name, port_attr.gid_tbl_len);

    struct ibv_qp *qp;
    qp = create_qp(pd, cq);
    print_qp_state(qp);

    // RESET -> INIT
    init_qp(qp);
    print_qp_state(qp);

    // INIT -> RTR -> RTS

    // (lid, qpn, psn, gid)
    rdma_dest_t my_dest;
    my_dest.lid = port_attr.lid;
    my_dest.qpn = qp->qp_num;
    my_dest.psn = lrand48() & 0xffffffff;

    int rc = ibv_query_gid(context, PORT_NUM, 1, &my_dest.gid);
    RDMA_CHECK(rc == 0, "ibv_query_gid() failed");

    char gid[33];
    memset(gid, 0, sizeof(gid));
    inet_ntop(AF_INET6, (void *)&my_dest.gid.raw, gid, sizeof(gid));
    printf(" Local address: LID: 0x%04x, QPN: 0x%06x, PSN: 0x%06x, GID: %s\n",
            my_dest.lid, my_dest.qpn, my_dest.psn, gid);
    printf("Subnet prefix = %llx, id = %llx\n", my_dest.gid.global.subnet_prefix, my_dest.gid.global.interface_id);

    char gid_buffer[34];
    serialize_gid(&my_dest.gid, gid_buffer);
    printf("serialized gid = %s\n", gid_buffer);

    union ibv_gid my_gid;
    deserialize_gid(gid_buffer, &my_gid);

    if (memcmp(&my_gid, &my_dest.gid, sizeof(my_gid)) != 0) {
        printf("deserialize error!\n");
    } else {
        printf("deserialize correct!\n");
    }

    //rdma_dest_t rem_dest;
    //inet_pton(AF_INET6, gid, (void*)&rem_dest.gid);
    

    // INIT -> RTR (Ready to Receive)
    // RTR -> RTS (Ready to Send)

    int is_server = 0;
    rdma_dest_t *rem_dest;
    if (is_server) {
        rem_dest = server_exchange_dest(&my_dest);
    } else {
        rem_dest = client_exchange_dest(&my_dest);
    }

    if (rem_dest) {
        printf("remote lid = %hx, qpn = %x, psn = %x\n", 
                rem_dest->lid, rem_dest->qpn, rem_dest->psn);

        printf("remote Subnet prefix = %llx, id = %llx\n", 
                rem_dest->gid.global.subnet_prefix, 
                rem_dest->gid.global.interface_id);
        pp_connect(qp, rem_dest, &my_dest);
        print_qp_state(qp);
    }

    ibv_destroy_cq(cq);
    ibv_destroy_comp_channel(channel);
    ibv_dealloc_pd(pd);
    ibv_close_device(context);
    ibv_free_device_list(device_list);
    return 0;
}