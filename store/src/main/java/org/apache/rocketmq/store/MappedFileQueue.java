/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

public class MappedFileQueue implements Swappable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    protected final String storePath; // 文件目录地址

    protected final int mappedFileSize; // 单个文件大小
    // CopyOnWriteArrayList线程安全的List
    protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>(); // 文件列表

    protected final AllocateMappedFileService allocateMappedFileService; // mappedFile服务类

    protected long flushedWhere = 0;
    protected long committedWhere = 0;

    protected volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() {
        List<MappedFile> mappedFiles = new ArrayList<>(this.mappedFiles);
        if (!mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 寻找最后修改时间大于给定时间戳的最早一个文件
     * 找不到就返回最后一个文件
     * @param timestamp
     * @return
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {      // 循环mappedFile列表
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    protected Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }


    public boolean load() {
        File dir = new File(this.storePath);
        File[] ls = dir.listFiles();
        if (ls != null) {
            return doLoad(Arrays.asList(ls));
        }
        return true;
    }

    public boolean doLoad(List<File> files) {
        // ascending order
        files.sort(Comparator.comparing(File::getName));

        for (File file : files) {
            if (file.length() != this.mappedFileSize) {
                log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                return false;
            }

            try {
                MappedFile mappedFile = new DefaultMappedFile(file.getPath(), mappedFileSize);

                mappedFile.setWrotePosition(this.mappedFileSize);
                mappedFile.setFlushedPosition(this.mappedFileSize);
                mappedFile.setCommittedPosition(this.mappedFileSize);
                this.mappedFiles.add(mappedFile);
                log.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                log.error("load file " + file + " error", e);
                return false;
            }
        }
        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            return tryCreateMappedFile(createOffset);
        }

        return mappedFileLast;
    }

    public boolean isMappedFilesEmpty() {
        return this.mappedFiles.isEmpty();
    }

    protected MappedFile tryCreateMappedFile(long createOffset) {
        String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset); // 创建新的文件地址？
        String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset
                + this.mappedFileSize);
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
        MappedFile mappedFile = null;

        if (this.allocateMappedFileService != null) {
            mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
        } else {
            try {
                mappedFile = new DefaultMappedFile(nextFilePath, this.mappedFileSize);
            } catch (IOException e) {
                log.error("create mappedFile exception", e);
            }
        }

        if (mappedFile != null) {
            if (this.mappedFiles.isEmpty()) {
                mappedFile.setFirstCreateInQueue(true);
            }
            this.mappedFiles.add(mappedFile);
        }

        return mappedFile;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    public MappedFile getLastMappedFile() {
        MappedFile[] mappedFiles = this.mappedFiles.toArray(new MappedFile[0]);
        return mappedFiles.length == 0 ? null : mappedFiles[mappedFiles.length - 1];
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator(mappedFiles.size());

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately,
        final int deleteFileBatchMax) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        int skipFileNum = 0;
        if (null != mfs) {
            //do check before deleting
            checkSelf();
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (skipFileNum > 0) {
                        log.info("Delete CommitLog {} but skip {} files", mappedFile.getFileName(), skipFileNum);
                    }
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= deleteFileBatchMax) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    skipFileNum++;
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffsetForTimerLog(long offset, int checkOffset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = false;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(checkOffset);
                try {
                    if (result != null) {
                        int position = result.getByteBuffer().position();
                        int size = result.getByteBuffer().getInt();//size
                        result.getByteBuffer().getLong(); //prev pos
                        int magic = result.getByteBuffer().getInt();
                        if (size == unitSize && (magic | 0xF) == 0xF) {
                            result.getByteBuffer().position(position + MixAll.UNIT_PRE_SIZE_FOR_MSG);
                            long maxOffsetPy = result.getByteBuffer().getLong();
                            destroy = maxOffsetPy < offset;
                            if (destroy) {
                                log.info("physic min commitlog offset " + offset + ", current mappedFile's max offset "
                                    + maxOffsetPy + ", delete it");
                            }
                        } else {
                            log.warn("Found error data in [{}] checkOffset:{} unitSize:{}", mappedFile.getFileName(),
                                checkOffset, unitSize);
                        }
                    } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                        log.warn("Found a hanged consume queue file, attempting to delete it.");
                        destroy = true;
                    } else {
                        log.warn("this being not executed forever.");
                        break;
                    }
                } finally {
                    if (null != result) {
                        result.release();
                    }
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    public synchronized boolean commit(final int commitLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere;
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                // offset 小于文件开始的offset   或者 offset 大于等于 最后一个文件的结尾offset
                // 也就是说明这个offset 在文件的范围内 不是是一个合法的offset
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
                } else {
                    // 合法的offset           // 第几个文件后面                第一个文件的起始offset 其实就是 -0
                    // 多个文件的时候 需要清理内存 减少压力，所以后面减的不一定是0
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    if (targetFile != null && offset >= targetFile.getFileFromOffset() // 大于这个文件开始的
                        && offset < targetFile.getFileFromOffset() + this.mappedFileSize) { // 小于这个文件结尾的
                        return targetFile;
                    }
                    // 找不到循环一遍看看是不是在 ？？？什么场景会走这个循环？ 看起来好像走不到  除非commit log文件被中间删掉了一个
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                            && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {

        if (mappedFiles.isEmpty()) {
            return;
        }

        if (reserveNum < 3) {
            reserveNum = 3;
        }

        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceSwapIntervalMs) {
                mappedFile.swapMap();
                continue;
            }
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > normalSwapIntervalMs
                    && mappedFile.getMappedByteBufferAccessCountSinceLastSwap() > 0) {
                mappedFile.swapMap();
                continue;
            }
        }
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {

        if (mappedFiles.isEmpty()) {
            return;
        }

        int reserveNum = 3;
        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceCleanSwapIntervalMs) {
                mappedFile.cleanSwapedMap(false);
            }
        }
    }

    public Object[] snapshot() {
        // return a safe copy
        return this.mappedFiles.toArray();
    }

    public Stream<MappedFile> stream() {
        return this.mappedFiles.stream();
    }

    public Stream<MappedFile> reversedStream() {
        return Lists.reverse(this.mappedFiles).stream();
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }

    public long getTotalFileSize() {
        return (long) mappedFileSize * mappedFiles.size();
    }
}
