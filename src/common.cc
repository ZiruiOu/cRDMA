#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <malloc.h>


#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>


// RDMA setup resource:
// device -> PD -> QP & CQ -> MR
// (a) open RDMA device .
// (b) allocate protection domain (PD) from device .
// (c) create qp from PD : `ibv_create_qp` .
// (e) RDMA setup connection
// (f) register memory region (MR) from PD
// (g) operations

struct RDMAContext {
    RDMAContext() = default;
    ~RDMAContext() = default;

    uint8_t deviceIdx;
    uint8_t portNum;

    struct ibv_context *ctx;
    struct ibv_pd *pd;
};

struct RDMAEndpoint {
    uint16_t lid;
    uint32_t qpn;
    uint32_t psn;
    union ibv_gid gid;
};

struct RDMARegion {
    uint64_t lAddr;
    uint64_t rAddr;
    uint32_t lKey;
    uint32_t rKey;
};

const int kRemoteBuffer = 4;

// Simple message to exchange
// lid, qpn, psn, gid, raddr & rkey
struct RDMAMsg{
    uint64_t addr;      // buffer address
    uint32_t rkey;      // remote key
    uint32_t qpn;       // queue pair number
    uint32_t psn;       // packet sequence number
    uint16_t lid;       // LID of the IB port
    uint8_t  gid[16];   // gid
} __attribute__((packed));

struct RDMAMeta{
    uint64_t addr[kRemoteBuffer];      // buffer address
    uint32_t rkey[kRemoteBuffer];      // remote key
    uint32_t qpn;       // queue pair number
    uint32_t psn;       // packet sequence number
    uint16_t lid;       // LID of the IB port
    uint8_t  gid[16];   // gid
} __attribute__((packed));


struct RDMAContext* createRDMAContext(const char *deviceName, uint8_t portNum) {
    int numDevice;
    struct ibv_device **deviceList;

    deviceList = ibv_get_device_list(&numDevice);
    int i;
    for (i = 0; i < numDevice; i++) {
        if (!strcmp(ibv_get_device_name(deviceList[i]), deviceName)) {
            break;
        }
    }

    struct RDMAContext *context = new RDMAContext();
    // open device
    // FIXME: check ctx != NULL
    struct ibv_context *ctx = ibv_open_device(deviceList[i]);
    // allocate protection domain
    // FIXME: check that pd != NULL
    struct ibv_pd *pd = ibv_alloc_pd(ctx);

    context->ctx = ctx;
    context->pd  = pd;
    context->deviceIdx = i;
    context->portNum = portNum;

    return context;
}

struct ibv_cq *createCQ(struct RDMAContext *context) {
    // create completion channel
    // FIXME: check that compChan != NULL
    struct ibv_comp_channel *compChan = ibv_create_comp_channel(context->ctx);

    // TODO: numCQE = ?
    struct ibv_cq *cq = ibv_create_cq(
        context->ctx, 128, nullptr, compChan, 0
    );

    return cq;
}

// RDMA create queue pair (QP)
struct ibv_qp * createQP(struct RDMAContext *context, struct ibv_cq *cq) {
    // create qp
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));

    // Queue pair configurations.
    attr.send_cq = cq;
    attr.recv_cq = cq;
    // TODO: max_send_wr/max_recv_wr = ?
    attr.cap.max_send_wr = 32;
    attr.cap.max_recv_wr = 32;
    attr.cap.max_send_sge = 30;
    attr.cap.max_recv_sge = 30;

    // Queue pair type: RC/RD/UD.
    // TODO: support UD (e.g. FaSST or eRPC)
    attr.qp_type = IBV_QPT_RC;
    struct ibv_qp *qp = ibv_create_qp(context->pd, &attr);
    // FIXME: check qp

    return qp;
}


// RDMA setup connection: 
// Init -> Ready to Receive (RTR) -> Ready to Send (RTS).
int modifyQPToInit(struct ibv_qp * qp, struct RDMAContext *ctx) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ctx->portNum;
    attr.pkey_index = 0;

    attr.qp_access_flags = (
        IBV_ACCESS_LOCAL_WRITE |
        IBV_ACCESS_REMOTE_WRITE |
        IBV_ACCESS_REMOTE_READ
    );

    int ret = ibv_modify_qp(
        qp, &attr, 
        IBV_QP_STATE        |
        IBV_QP_PORT         |
        IBV_QP_PKEY_INDEX   |
        IBV_QP_ACCESS_FLAGS
    );
    
    return ret;
}


int modifyQPToRTR(struct ibv_qp * qp, 
                //struct RDMAEndpoint *rAddr, 
                uint32_t rPSN, uint32_t rQPN,
                uint16_t rLID, union ibv_gid gid,
                struct RDMAContext *ctx) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    //attr.qp_state = IBV_QPS_RTR;
    //attr.path_mtu = IBV_MTU_512;
    //attr.rq_psn = 
    attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_512,
        //.rq_psn = rAddr->psn,
        //.dest_qp_num = rAddr->qpn,
        .rq_psn = rPSN,
        .dest_qp_num = rQPN,
        .ah_attr = {
            //.dlid = rAddr->lid,
            .dlid = rLID,
            .sl = 0,
            .src_path_bits = 0,
            .is_global = 1,
            .port_num = ctx->portNum
        },
        .max_dest_rd_atomic = 1,
        .min_rnr_timer = 12,
    };

    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.grh.dgid = gid;
    // FIXME: configure it to be the same as "ibv_query_gid".
    attr.ah_attr.grh.sgid_index = 3; 

    int ret = ibv_modify_qp(
        qp, &attr, 
        IBV_QP_STATE |
        IBV_QP_AV    |
        IBV_QP_PATH_MTU |
        IBV_QP_RQ_PSN   |
        IBV_QP_DEST_QPN |
        IBV_QP_RQ_PSN   |
        IBV_QP_MAX_DEST_RD_ATOMIC |
        IBV_QP_MIN_RNR_TIMER
    );

    if (ret < 0) {
        fprintf(stderr, "rdma_connect: ibv_modify_qp fail to convert INIT->RTR ...");
    }

    return ret;
}

int modifyQPToRTS(struct ibv_qp *qp, 
                  //struct RDMAEndpoint *lAddr
                  uint32_t lPSN
                  ) {
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    //attr.sq_psn = lAddr->psn;
    attr.sq_psn = lPSN;
    attr.max_rd_atomic = 1;

    int ret = ibv_modify_qp(
        qp, &attr, 
        IBV_QP_STATE        |
        IBV_QP_TIMEOUT      |
        IBV_QP_RETRY_CNT    |
        IBV_QP_RNR_RETRY    |
        IBV_QP_SQ_PSN       |
        IBV_QP_MAX_QP_RD_ATOMIC
    );

    return ret;
}

void queryQPState(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr initAttr;
    memset(&attr, 0, sizeof(attr));

    int ret = ibv_query_qp(qp, &attr, IBV_QP_STATE, &initAttr);

    switch(attr.qp_state) {
        case IBV_QPS_RESET:
            printf("QP State: Reset\n");
            break;
        case IBV_QPS_ERR:
            printf("QP State: Error\n");
            break;
        case IBV_QPS_INIT:
            printf("QP State: Init\n");
            break;
        case IBV_QPS_RTR:
            printf("QP State: Ready to Receive\n");
            break;
        case IBV_QPS_RTS:
            printf("QP State: Ready to Send\n");
            break;
        default:
            break;
    }
}

void fillSgeAndWR(ibv_sge &sge, ibv_send_wr &wr, uint64_t buffAddr, 
                  uint32_t length, uint32_t lKey) {
    sge.addr = (uintptr_t)buffAddr;
    sge.length = length;
    sge.lkey = lKey;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 0;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED; // generate one cqe elements after completion.
}

int RDMASend(ibv_qp *qp, uintptr_t buffAddr, uint32_t length, uint32_t lKey) {
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *badWr;

    fillSgeAndWR(sge, wr, buffAddr, length, lKey);

    // FIXME: check out ret.
    int ret = ibv_post_send(qp, &wr, &badWr);
    return ret;
}

int RDMAReceive(ibv_qp *qp, uintptr_t bufAddr, uint32_t length, uint32_t lKey) {
    struct ibv_sge sge;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *badWr;

    sge.addr = bufAddr;
    sge.length = length;
    sge.lkey = lKey;

    memset(&wr, 0, sizeof(wr));
    // TODO: wait for specific `wr_id`.
    wr.wr_id = 0;
    wr.num_sge = 1;
    wr.sg_list = &sge;

    // FIXME: check ret.
    int ret = ibv_post_recv(qp, &wr, &badWr);
    return ret;
}

int RDMARead(ibv_qp *qp, uintptr_t buffAddr, uint32_t length, uint32_t lKey,
             uint64_t remoteAddr, uint32_t rKey) {
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *badWr;

    sge.addr = buffAddr;
    sge.length = length;
    sge.lkey = lKey;

    memset(&wr, 0, sizeof(wr));
    wr.num_sge = 1;
    wr.sg_list = &sge;

    wr.opcode = IBV_WR_RDMA_READ;
    wr.wr_id = 0; // TODO
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remoteAddr;
    wr.wr.rdma.rkey = rKey;

    int rc = ibv_post_send(qp, &wr, &badWr);
    return rc;
}

int RDMAWrite(ibv_qp *qp, uintptr_t buffAddr, uint32_t length, uint32_t lKey, 
              uint64_t remoteAddr, uint32_t rKey, int imm) {

    struct ibv_sge sge;
    struct ibv_send_wr sr;
    struct ibv_send_wr *badWr;

    sge.addr = (uint64_t) buffAddr;
    sge.length = length;
    sge.lkey = lKey;

    memset(&sr, 0, sizeof(sr));
    sr.wr_id = 0;
    sr.next  = nullptr;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.send_flags = IBV_SEND_SIGNALED;
    if (imm > -1) {
        sr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM; 
        sr.imm_data = imm;
    } else {
        sr.opcode = IBV_WR_RDMA_WRITE;
    }

    sr.wr.rdma.remote_addr = remoteAddr;
    sr.wr.rdma.rkey = rKey;

    int rc = ibv_post_send(qp, &sr, &badWr);
    if (rc) {
        printf("RDMAWrite error!\n");
    }
    return rc;
}

int RDMAWriteBatch(ibv_qp *qp, char *buffers[kRemoteBuffer], int length, uint32_t lKeys[kRemoteBuffer],
                   uint64_t rAddrs[kRemoteBuffer], uint32_t rKeys[kRemoteBuffer], int imm) {
    struct ibv_sge sgeList[kRemoteBuffer];
    struct ibv_send_wr wrs[kRemoteBuffer];
    struct ibv_send_wr *badWr;

    for (int i = 0; i < kRemoteBuffer; i++) {
        sgeList[i].addr = (uint64_t) buffers[i];
        sgeList[i].length = length;
        sgeList[i].lkey = lKeys[i];

        memset(&wrs[i], 0, sizeof(wrs[i]));
        wrs[i].sg_list = &sgeList[i];
        wrs[i].num_sge = 1;

        // NOTE: doorbell batching .
        wrs[i].wr_id = 0;
        wrs[i].next = (i == kRemoteBuffer - 1) ? NULL : &wrs[i+1];
        wrs[i].opcode = IBV_WR_RDMA_WRITE;

        wrs[i].wr.rdma.remote_addr = rAddrs[i];
        wrs[i].wr.rdma.rkey = rKeys[i];

        if (i == kRemoteBuffer - 1) {
            // NOTE: only the last write would generate CQE.
            wrs[i].send_flags = IBV_SEND_SIGNALED;
            if (imm > -1) {
                wrs[i].opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
                wrs[i].imm_data = imm;
            }
        }
    }

    int rc = ibv_post_send(qp, &wrs[0], &badWr);
    return rc;
}

int PollWithCQ(ibv_cq *cq, int numSucc, ibv_wc *wc) {
    int cnt = 0;
    do {
        int ret = ibv_poll_cq(cq, 1, wc);
        cnt += ret;
    } while (cnt < numSucc);

    if (wc->status != IBV_WC_SUCCESS) {
        // FIXME: 
        return -1;
    }
    return cnt;
}

const char server_name[] = "172.16.112.43";
const int port = 7788;

int GetAvailableSocket(int isServer) {
    struct addrinfo *res;
    struct addrinfo hints = {
        .ai_family = AF_UNSPEC,
        .ai_socktype = SOCK_STREAM,
    };
    if (isServer) {
        hints.ai_flags = AI_PASSIVE;
    }

    int sockfd;
    char service[5];
    memset(service, 0, sizeof(service));
    sprintf(service, "%d", port);

    int n = getaddrinfo(server_name, service, &hints, &res);
    for(struct addrinfo *t = res; t; t = t->ai_next) {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 1) {
            n = 1;
            if (isServer) {
                setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);
                if (!bind(sockfd, t->ai_addr, t->ai_addrlen)) {
                    break;
                }
            } else {
                if (!connect(sockfd, t->ai_addr, t->ai_addrlen)) {
                    break;
                }
            }
            close(sockfd);
            sockfd = -1;
        }
    }
    freeaddrinfo(res);
    return sockfd;
}


// socket connection setup.
int ServerConnect() {
    int sockfd, connfd;
    sockfd = GetAvailableSocket(1);

    listen(sockfd, 1);
    connfd = accept(sockfd, NULL, NULL);
    close(sockfd);

    return connfd;
}

int ClientConnect() {
    return GetAvailableSocket(0);
}

int SocketRead(int sock, char *buffer, int length) {
    int ret = 0;
    int totalReadBytes = 0;

    while(!ret && totalReadBytes < length) {
        ret = read(sock, buffer + totalReadBytes, length - totalReadBytes);
        if (ret == -1) {
            // socket read error.
            return -1;
        }
        if (ret == 0) {
            // End of file or connection failed.
            break;
        }
        totalReadBytes += ret;
    }
    return totalReadBytes;
}


int main(int argc, char* argv[]) {
    //char deviceName[7] = "mlx5_0";
    char *deviceName;
    uint8_t portNum = 1;

    int o, isServer;

    isServer = 0;
    const char *optString = "si:";

    while ((o = getopt(argc, argv, optString)) != -1) {
        switch (o) {
            case 's':
                isServer = 1;
                printf("Set as server\n");
                break;
            case 'i':
                deviceName = optarg;
                break;
        }
    }

    RDMAContext *context = createRDMAContext(deviceName, portNum);

    // create queue pair.
    struct ibv_cq *cq = createCQ(context);
    struct ibv_qp *qp = createQP(context, cq);
    modifyQPToInit(qp, context);
    queryQPState(qp);

    // create memory region.
    //int length = 32 * 1024;
    ////char *buffer = (char *)malloc(length);
    //char *buffer = (char*)memalign(1024, length);

    //if (!isServer) {
    //    char wow[] = "Hello, My name is Shinonome Ena. Kon no sekai wa bye bye bye bye ...";
    //    memcpy(buffer, wow, sizeof(wow));
    //}

    int length = 32 * 1024;
    struct ibv_mr *mr[kRemoteBuffer];
    for (int i = 0; i < kRemoteBuffer; i++) {
        char *buffer = (char*)memalign(1024, length);
        mr[i] = ibv_reg_mr(
            context->pd, buffer, length,
            IBV_ACCESS_LOCAL_WRITE
            | IBV_ACCESS_REMOTE_WRITE
            | IBV_ACCESS_REMOTE_READ
        );
    }

    //struct ibv_mr *mr = ibv_reg_mr(context->pd, buffer, length, 
    //                                IBV_ACCESS_LOCAL_WRITE 
    //                                | IBV_ACCESS_REMOTE_READ 
    //                                | IBV_ACCESS_REMOTE_WRITE);
    // lkey: For local HCA access admission check.
    // rkey: For remote HCA access admission check.
    //printf("Local address: %p, mr address: %p", buffer, mr->addr);

    // negotiate RDMAEndpoint.
    // TODO: refactor/remove.
    struct ibv_port_attr attr;
    int rc = ibv_query_port(context->ctx, context->portNum, &attr);

    //struct RDMAMsg local, remote, temp;

    struct RDMAMeta local, remote, temp;

    // serialize local buffer address, rkey, and connections.
    for (int i = 0; i < kRemoteBuffer; i++) {
        local.addr[i] = (uint64_t)mr[i]->addr;
        local.rkey[i] = mr[i]->rkey;

        // fill up temp.
        temp.addr[i] = htobe64((uint64_t)mr[i]->addr);
        temp.rkey[i] = htobe32(mr[i]->rkey);
        printf("Local addr #%d: %lx, rkey: %x\n", i, local.addr[i], local.rkey[i]);
    }
    local.lid = attr.lid;
    local.qpn = qp->qp_num;
    local.psn = (lrand48() & 0xffffffff);
    rc = ibv_query_gid(context->ctx, context->portNum, 3,
                       (ibv_gid*)&local.gid);
    if (rc == -1)
        printf("Query Gid failed.\n");

    printf("Local gid: ");
    for (int i = 0; i < 8; i++) {
        printf("%x%x:", local.gid[2*i], local.gid[2*i+1]);
    }
    printf("\n");

    temp.lid = htobe16(local.lid);
    temp.qpn = htobe32(local.qpn);
    temp.psn = htobe32(local.psn);
    memcpy((void*)&temp.gid, (void*)local.gid, sizeof(temp.gid));

    
    // exchange through socket.
    int fd;
    if (isServer) {
        fd = ServerConnect();
        int n = write(fd, &temp, sizeof(temp));
        if (n != sizeof(temp)) {
            printf("Server: fail to send metadata");
        }
        n = SocketRead(fd, (char*)&remote, sizeof(remote));
        if (n != sizeof(remote)) {
            printf("Server: fail to receive metadata");
        }
        printf("Server: ok to exchange metadata\n");

    } else {
        fd = ClientConnect();
        int n = SocketRead(fd, (char*)&remote, sizeof(remote));
        if (n != sizeof(remote)) {
            printf("Client: fail to receive metadata");
        }
        n = write(fd, &temp, sizeof(temp));
        if (n != sizeof(temp)) {
            printf("Client: fail to receive metadata");
        }
        printf("Client: ok to exchange metadata\n");
    }

    // deserialize
    for (int i = 0; i < kRemoteBuffer; i++) {
        remote.addr[i] = be64toh(remote.addr[i]);
        remote.rkey[i] = be32toh(remote.rkey[i]);
        printf("Remote addr #%d: %lx, rkey: %x\n", i, remote.addr[i], remote.rkey[i]);
    }
    remote.lid =  be16toh(remote.lid);
    remote.qpn =  be32toh(remote.qpn);
    remote.psn =  be32toh(remote.psn);

    printf("Local lid: %x, qpn: %x, psn: %x\n", 
            local.lid, local.qpn, local.psn);
    printf("Remote lid: %x, qpn: %x, psn: %x\n", 
            remote.lid, remote.qpn, remote.psn);

    union ibv_gid rgid;
    memcpy(&rgid, remote.gid, sizeof(rgid));

    printf("Remote gid: ");
    for (int i = 0; i < 8; i++) {
        printf("%x%x:", rgid.raw[2*i], rgid.raw[2*i+1]);
    }
    printf("\n");

    modifyQPToRTR(qp, remote.psn, remote.qpn, remote.lid, rgid, context);
    modifyQPToRTS(qp, local.psn);
    queryQPState(qp);

    // post some empty receives ahead of time.
    if (isServer) {
        struct ibv_sge sge;
        struct ibv_recv_wr wr;
        struct ibv_recv_wr *badWr;

        memset(&sge, 0, sizeof(sge));

        memset(&wr, 0, sizeof(wr));
        wr.wr_id = 0;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        int rc = ibv_post_recv(qp, &wr, &badWr);
        if (rc) {
            printf("Fail to post receive request");
        }
    }

    usleep(4);

    if (isServer) {
        // Empty receive.

        struct ibv_wc wc;
        int n = PollWithCQ(cq, 1, &wc);
        if (wc.status != IBV_WC_SUCCESS) {
            printf("Server: poll completion queue fails\n");
        }
        //printf("Received immediate: %d\n", wc.imm_data);

        for (int i = 0; i < kRemoteBuffer; i++) {
            printf("Receive #%d buffer: %s", i, (char*) mr[i]->addr);
        }

    } else {

        // Generate content.
        char * buffers[kRemoteBuffer];
        uint32_t lKeys[kRemoteBuffer];
        for (int i = 0; i < kRemoteBuffer; i ++) {
            buffers[i] = (char*) mr[i]->addr;
            lKeys[i] = mr[i]->lkey;
            sprintf(buffers[i], 
                    "#%d: Hello, This is Shinonome Ena. Kono Sekai wa bye bye bye bye ...\n", 
                    i + 1);
        }

        RDMAWriteBatch(
            qp, buffers, length, lKeys, 
            remote.addr, remote.rkey, 114514
        );
        //RDMAWrite(
        //    qp, (uintptr_t) buffers[0], length, lKeys[0],
        //    remote.addr[0], remote.rkey[0], 114514
        //);

        // poll cq.
        struct ibv_wc wc;
        memset(&wc, 0, sizeof(wc));
        wc.wr_id = 0;
        int n = PollWithCQ(cq, 1, &wc);
        if (wc.status != IBV_WC_SUCCESS) {
            printf("Client: poll completion queue fails. status code = %d\n",wc.status);
            printf("Status code = %d\n", wc.status);
            if (wc.status == IBV_WC_LOC_ACCESS_ERR)
                printf("Client: Location access error.\n");
            if (wc.status == IBV_WC_RETRY_EXC_ERR) {
                printf("Client: Retry execution error.\n");
            }
        }
        //printf("Received message : %s\n", buffer);
    }

    return 0;
}
