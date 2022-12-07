package clickhouse.mysqlengine.odbc.proxy;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.nio.ByteBuffer;

/**
 * @author shaoyunchuan
 * @date 2022/11/24
 */
public class ClickHouseMysqlEngineODBCHandler {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseMysqlEngineODBCHandler.class);

    private byte[] data;// 数据集

    private int remain;

    private int type;// 0:普通 1:错误报文

    private boolean hasCompleted;

    private int rowSetIdx;

    private int eofScannerIdx;

    public ClickHouseMysqlEngineODBCHandler() {
        this.data = new byte[0];
        this.hasCompleted = false;
    }

    public void append(byte[] data) {
        if (hasCompleted()) {
            logger.error("not full data!");
            throw new RuntimeException("not full data!");
        }

        byte[] oldData = this.data;
        byte[] newData = new byte[oldData.length + data.length];
        System.arraycopy(oldData,0,newData,0,oldData.length);
        System.arraycopy(data,0,newData,oldData.length,data.length);
        this.data = newData;
        if (hasError()) {
            if (this.remain <= 0) {
                this.hasCompleted = true;
            }
        }
        if (hasEOF()) {
            if (this.remain <= 0) {
                this.hasCompleted = true;
            }
        } else {
            if (this.remain > 0) {
                this.remain -= data.length;
            }
        }
    }

    public boolean isEmpty() {
        return this.data.length == 0;
    }

    public byte[] read() {
        return this.data;
    }

    public void clear() {
        this.hasCompleted = false;
        this.type = 0;
        this.data = new byte[0];
        this.rowSetIdx = 0;
        this.eofScannerIdx = 0;
    }

    public boolean hasCompleted() {
        return this.hasCompleted;
    }

    /**
     * 判断是否携带eof标记，并记录附带信息长度
     * @return
     */
    private boolean hasEOF() {
        int idx = skipMetaData();
        byte[] tmp = this.data;
        if (idx < 0) {
            return false;
        }
        idx = Math.max(this.eofScannerIdx, idx);
        while (idx + 4 < tmp.length) {
            if (tmp[idx + 4] == -2 && tmp[idx] < 16777215) {
                this.remain = (tmp[idx] & 0xff) + ((tmp[idx + 1] & 0xff) << 8) + ((tmp[idx + 2] & 0xff) << 16) - (tmp.length - idx - 4);
                return true;
            }
            if((idx = skipPackets(idx, tmp)) < 0) {
                return false;
            }
        }
        this.eofScannerIdx = idx;
        return false;
    }

    /**
     * 核心方法，为完整的数据集补充字段最大长度信息，并设置字段类型为utf8
     */
    public void handler() {
        if (!this.hasCompleted) {
            throw new RuntimeException("not full data!");
        }
        if (this.type > 0) {
            return;
        }

        byte[] tmp = this.data;
        int columnNum = readColumnNum();
        int idx = 0;
        idx = skipPackets(idx,tmp);
        int idxForSkip = idx;

        long[] maxLengths = readColumnMaxLength();

        for (int i = 0; i < columnNum; i++) {
            idx+=4;
            idx = skipPara(idx, tmp);
            idx = skipPara(idx, tmp);
            idx = skipPara(idx, tmp);
            idx = skipPara(idx, tmp);
            idx = skipPara(idx, tmp);
            idx = skipPara(idx, tmp);
            idx++;// 12
            tmp[idx] = 45;// 字符集

            idx++;
            idx++;

            byte[] bites = toBites(maxLengths[i]);

            tmp[idx] = bites[3];
            tmp[idx + 1] = bites[2];
            tmp[idx + 2] = bites[1];
            tmp[idx + 3] =  bites[0];

            idx += 4;
            tmp[idx] = -3;
            idx += 2;
            tmp[idx] =  31;

            idx = skipPackets(idxForSkip,tmp);// 不使用游标
            idxForSkip = idx;
        }


    }

    /**
     * 获取数据集字段数，若-1则为错误报文
     * @return
     */
    private int readColumnNum() {
        return this.data[4];
    }

    /**
     * 返回每个字段的最大值
     * @return
     */
    private long[] readColumnMaxLength() {
        byte[] tmp = this.data;
        int maxLength = this.data.length;
        int columnNum = readColumnNum();
        long[] columnMaxLength = new long[columnNum];

        if (!this.hasCompleted) {
            logger.error("数据不完整!");
            return columnMaxLength;
        }

        int idx = skipMetaData();
        if (idx < 0) {
            logger.error("获取最大长度失败!");
            return columnMaxLength;
        }
        // 按行跳转
        int idxForSkip = idx;
        while (idx < maxLength) {
            int remaining = tmp[idx]; // 废弃
            idx+=4;
            int rw = tmp[idx];
            // EOF报文
            if (rw == -2 && remaining < 16777215) {
                break;
            }

            // 错误码
            if (rw == -1) {
                break;
            }

            for (int i = 0; i < columnNum; i++) {
                int len = 0;
                rw = tmp[idx];

                switch (rw) {
                    case -5: // null
                        break;
                    case -4:
                        len = (tmp[idx + 1] & 0xff) | ((tmp[idx + 2] & 0xff) << 8);
                        break;
                    case -3:
                        len = (tmp[idx + 1] & 0xff) | ((tmp[idx + 2] & 0xff) << 8) | ((tmp[idx + 3] & 0xff) << 16);
                        break;
                    case -2:
                        len = (int) ((tmp[idx + 1] & 0xff) | ((long) (tmp[idx + 2] & 0xff) << 8)
                                | ((long) (tmp[idx + 3] & 0xff) << 16) | ((long) (tmp[idx + 4] & 0xff) << 24)
                                | ((long) (tmp[idx + 5] & 0xff) << 32) | ((long) (tmp[idx + 6] & 0xff) << 40)
                                | ((long) (tmp[idx + 7] & 0xff) << 48) | ((long) (tmp[idx + 8] & 0xff) << 56));
                        break;
                    default:
                        len = rw;
                }

//              使用for + if循环替换原版方法 columnMaxLength[i] = Math.max(columnMaxLength[i] ,new String(tmp,idx + 1,len).length())
                long num = num(tmp, idx + 1, len);
                if (columnMaxLength[i] < num) {
                    columnMaxLength[i] = num;
                }

                idx = skipPara(idx,tmp);
            }

            idx = skipPackets(idxForSkip,tmp);
            idxForSkip = idx;
        }

        for (int i = 0; i < columnMaxLength.length; i++) {
            columnMaxLength[i] = columnMaxLength[i] * 4L;
        }

        return columnMaxLength;
    }

    /**
     * 字节数组转字符个数
     * @param chars
     * @param off
     * @param len
     * @return
     */
    private long num(byte[] chars,int off,int len) {
        long num = 0L;
        int idx = off;
        int rowLen = idx + len;
        while (idx < rowLen) {
            if (chars[idx] < 0) {
                idx += 3;
            } else {
                ++idx;
            }
            ++num;
        }

        return num;
    }

    /**
     * 返回跳过字段信息后的指针 -1意味着报文并不完整
     * @return
     */
    private int skipMetaData() {
        if (this.rowSetIdx > 0) {
            return this.rowSetIdx;
        }
        int idx = 0;
        int maxLength = this.data.length;
        int columnNum = readColumnNum();
        // 跳过字段数报文
        idx = skipPackets(idx, this.data);
        if (idx >= maxLength) {
            return -1;
        }
        for (int i = 0; i < columnNum; i++) {
            idx = skipPackets(idx, this.data);
            if (idx >= maxLength) {
                return -1;
            }
        }
        this.rowSetIdx = idx;
        return idx;
    }

    /**
     * 跳过Packets
     * @param begin
     * @param bytes
     * @return
     */
    private int skipPackets(int begin, byte[] bytes) {
        return begin + 4 + (bytes[begin] & 0xff) + ((bytes[begin + 1] & 0xff) << 8) + ((bytes[begin + 2] & 0xff) << 16);
    }

    /**
     * 跳过字符串
     * @param begin
     * @param bytes
     * @return
     */
    private int skipPara(int begin, byte[] bytes) {
        return begin + 1 + bytes[begin];
    }

    /**
     * 将long型转换为4字节数组
     * @param value
     * @return
     */
    private byte[] toBites(long value) {
        byte[] bites = new byte[4];
        System.arraycopy(ByteBuffer
                .allocate(Long.SIZE / Byte.SIZE)
                .putLong(value)
                .array(),4,bites,0,4);
        return bites;
    }

    /**
     * 返回报文是否为错误报文，若为错误报文，则记录剩余字段长度即可，无需后续处理
     * @return
     */
    private boolean hasError() {
        byte[] tmp = this.data;
        if (readColumnNum() < 0) {
            if (this.remain < 1) {
                this.type = 1;
                this.remain = (tmp[0] & 0xff) + ((tmp[1] & 0xff) << 8) + ((tmp[2] & 0xff) << 16) - (tmp.length - 4);
            }
            return true;
        }
        return false;
    }

}

