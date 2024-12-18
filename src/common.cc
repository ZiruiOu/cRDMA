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
    attr.ah_attr.grh.sgid_index = 0;

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
    wr.wr_id = 0;
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
    char deviceName[7] = "mlx5_0";
    uint8_t portNum = 1;

    int o, isServer;

    isServer = 0;
    while ((o = getopt(argc, argv, "s")) != -1) {
        switch (o) {
            case 's':
                isServer = 1;
                printf("Set as server\n");
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
    int length = 32 * 1024;
    //char *buffer = (char *)malloc(length);
    char *buffer = (char*)memalign(1024, length);

    if (isServer) {
        char wow[] = "Hello, My name is Shinonome Ena.";
        memcpy(buffer, wow, sizeof(wow));
    }

    struct ibv_mr *mr = ibv_reg_mr(context->pd, buffer, length, 
                                    IBV_ACCESS_LOCAL_WRITE 
                                    | IBV_ACCESS_REMOTE_READ 
                                    | IBV_ACCESS_REMOTE_WRITE);
    // lkey: For local HCA access admission check.
    // rkey: For remote HCA access admission check.
    printf("Local address: %p, mr address: %p", buffer, mr->addr);

    // negotiate RDMAEndpoint.
    // TODO: refactor/remove.
    struct ibv_port_attr attr;
    int rc = ibv_query_port(context->ctx, context->portNum, &attr);


    struct RDMAMsg lMeta, rMeta, temp;

    // serialize local buffer address, rkey, and connections.
    lMeta.addr = ((uint64_t)mr->addr);
    lMeta.rkey =(mr->rkey);
    lMeta.lid = (attr.lid);
    lMeta.qpn = (qp->qp_num);
    lMeta.psn = (lrand48() & 0xffffffff);
    rc = ibv_query_gid(context->ctx, 
                       context->portNum, 
                       context->deviceIdx, 
                       (ibv_gid*) &lMeta.gid);

    temp.addr = htobe64((uint64_t)buffer);
    temp.rkey = htobe32(mr->rkey);
    temp.lid = htobe16(attr.lid);
    temp.qpn = htobe32(qp->qp_num);
    temp.psn = htobe32(lMeta.psn);
    memcpy(&temp.gid, &lMeta.gid, sizeof(temp.gid));

    printf("My remote key : %x\n", mr->rkey);
    
    // exchange through socket.
    int fd;
    if (isServer) {
        fd = ServerConnect();
        int n = write(fd, &temp, sizeof(temp));
        if (n != sizeof(temp)) {
            printf("Server: fail to send metadata");
        }
        n = SocketRead(fd, (char*)&rMeta, sizeof(rMeta));
        if (n != sizeof(rMeta)) {
            printf("Server: fail to receive metadata");
        }
        printf("Server: ok to exchange metadata\n");

    } else {
        fd = ClientConnect();
        int n = SocketRead(fd, (char*)&rMeta, sizeof(rMeta));
        if (n != sizeof(rMeta)) {
            printf("Client: fail to receive metadata");
        }
        n = write(fd, &temp, sizeof(temp));
        if (n != sizeof(temp)) {
            printf("Client: fail to receive metadata");
        }
        printf("Client: ok to exchange metadata\n");
    }
    
    rMeta.addr = be64toh(rMeta.addr);
    rMeta.rkey = be32toh(rMeta.rkey);
    rMeta.lid =  be16toh(rMeta.lid);
    rMeta.qpn =  be32toh(rMeta.qpn);
    rMeta.psn =  be32toh(rMeta.psn);

    printf("Local addr: %lx, rkey: %x, lid: %x, qpn: %x\n", 
            lMeta.addr, lMeta.rkey, lMeta.lid, lMeta.qpn);
    printf("Remote addr: %lx, rkey: %x, lid: %x, qpn: %x\n", 
            rMeta.addr, rMeta.rkey, rMeta.lid, rMeta.qpn);


    union ibv_gid rgid;
    memcpy(&rgid, rMeta.gid, sizeof(rgid));

    //printf("Remote gid: ");
    //for (int i = 0; i < 8; i++) {
    //    printf("%x%x:", rgid.raw[2*i], rgid.raw[2*i+1]);
    //}
    //printf("\n");

    modifyQPToRTR(qp, rMeta.psn, rMeta.qpn, rMeta.lid, rgid, context);
    modifyQPToRTS(qp, lMeta.psn);
    queryQPState(qp);

    // post some empty receives ahead of time.
    //if (isServer) {
    //    struct ibv_sge sge;
    //    struct ibv_recv_wr wr;
    //    struct ibv_recv_wr *badWr;

    //    memset(&sge, 0, sizeof(sge));

    //    memset(&wr, 0, sizeof(wr));
    //    wr.wr_id = 0;
    //    wr.sg_list = &sge;
    //    wr.num_sge = 1;
    //    int n = ibv_post_recv(qp, &wr, &badWr);
    //    if (n) {
    //        printf("Fail to post receive request");
    //    }
    //}

    sleep(4);


    if (isServer) {
        // Empty receive.

        //struct ibv_wc wc;
        //int n = PollWithCQ(cq, 1, &wc);
        //if (wc.status != IBV_WC_SUCCESS) {
        //    printf("Server: poll completion queue fails\n");
        //}

        while (1);

    } else {
        //char msg[] = "Hello, This is message from Ena Shinonome.";
        //memcpy(buffer, msg, sizeof(msg));

        RDMARead(qp, (uintptr_t) buffer, length, 
            mr->lkey, rMeta.addr, rMeta.rkey);

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

        printf("Received message : %s\n", buffer);
    }

    return 0;
}