#include <cstdio>
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

// RDMA create queue pair (QP)
struct ibv_qp * createQP(struct RDMAContext *context) {
    // create qp
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));

    // create completion channel
    // FIXME: check that compChan != NULL
    struct ibv_comp_channel *compChan = ibv_create_comp_channel(context->ctx);

    // TODO: numCQE = ?
    struct ibv_cq *cq = ibv_create_cq(
        context->ctx, 128, nullptr, compChan, 0
    );

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
                struct RDMAEndpoint *rAddr, 
                struct RDMAContext *ctx) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    //attr.qp_state = IBV_QPS_RTR;
    //attr.path_mtu = IBV_MTU_512;
    //attr.rq_psn = 
    attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_512,
        .rq_psn = rAddr->psn,
        .dest_qp_num = rAddr->qpn,
        .ah_attr = {
            .dlid = rAddr->lid,
            .sl = 0,
            .src_path_bits = 0,
            .is_global = 1,
            .port_num = ctx->portNum
        },
        .max_dest_rd_atomic = 1,
        .min_rnr_timer = 12,
    };

    int ret = ibv_modify_qp(
        qp, &attr, 
        IBV_QP_STATE |
        IBV_QP_PATH_MTU |
        IBV_QP_RQ_PSN   |
        IBV_QP_DEST_QPN |
        IBV_QP_RQ_PSN   |
        IBV_QP_MAX_DEST_RD_ATOMIC |
        IBV_QP_MIN_RNR_TIMER
    );

    return ret;
}

int modifyQPToRTS(struct ibv_qp *qp, struct RDMAEndpoint *lAddr) {
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = lAddr->psn;
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


int main() {
    char deviceName[7] = "mlx5_0";
    uint8_t portNum = 1;

    RDMAContext *context = createRDMAContext(deviceName, portNum);

    struct ibv_qp *qp = createQP(context);
    modifyQPToInit(qp, context);
    queryQPState(qp);

    // negotiate RDMAEndpoint.


    return 0;
}