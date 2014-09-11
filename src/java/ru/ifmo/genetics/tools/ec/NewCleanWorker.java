package ru.ifmo.genetics.tools.ec;

import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;

import ru.ifmo.genetics.io.MultiFile2MemoryMap;
import ru.ifmo.genetics.io.RandomAccessMultiFile;
import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.dna.kmers.ShortKmer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class NewCleanWorker implements Runnable {

    static int readCorrectingTimes = 1;

    final CountDownLatch latch;
    final NewCleanDispatcher dispatcher;
    final MultiFile2MemoryMap mf;
    final int len;
    final Long2ByteMap times;

    LongSet set;
    boolean interrupted = false;

    final int err;

    public NewCleanWorker(CountDownLatch latch, NewCleanDispatcher dispatcher,
                          MultiFile2MemoryMap mf, int len, Long2ByteMap times, int err) throws IOException {
        this.latch = latch;
        this.dispatcher = dispatcher;
        this.mf = mf;
        this.len = len;
        this.times = times;
        this.err = err;
    }

    public void interrupt() {
        interrupted = true;
    }

    @Override
    public void run() {
        while (!interrupted) {
            List<byte[]> list = dispatcher.getWorkRange(times);
            if (list == null) {
                break;
            }
            for (byte[] ar : list) {
                try {
                    processKmer(len, ar, mf, times);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        latch.countDown();
    }

    public void processKmer(int len, byte[] array, MultiFile2MemoryMap mf, Long2ByteMap times) throws IOException {
        List<byte[]> dnaqs = new ArrayList<byte[]>();
        IntList starts = new IntArrayList();
        LongList poses = new LongArrayList();
        IntList lengths = new IntArrayList();
        BooleanList fws = new BooleanArrayList();
        long kmer = 0;
        for (int i = 0; i < 8; ++i) {
            int x = array[i];
            if (x < 0) x += 256;
            kmer = (kmer << 8) + x;
        }
        boolean verbose = false;
        for (int i = 8; i < array.length; i += 8) {
            long pos = 0;
            for (int j = 0; j < 8; ++j) {
                int x = array[i + j];
                if (x < 0) x += 256;
                pos = (pos << 8) + x;
            }
            byte old = times.containsKey(pos) ? times.get(pos) : 0;
            if (old > readCorrectingTimes) {
                ++Kmer2ReadIndexBuilder.uncorrected;
                continue;
            }
            synchronized (times) {
                times.put(pos, (byte)(old + 1));
            }
            byte[] dnaqBytes = readDnaQ(mf, pos);
            int dnaqLen = dnaqBytes.length;
            DnaQ d = new DnaQ(dnaqBytes);
            int inReadPos = 0;
            int mind = Integer.MAX_VALUE, ind = -1;
            for (ShortKmer sk : ShortKmer.kmersOf(d, len)) {
                long fwKmer = sk.fwKmer();
                long rcKmer = sk.rcKmer();
                int d1 = dist(kmer, fwKmer, len);
                int d2 = dist(kmer, rcKmer, len);
                int cd = d1 < d2 ? d1 : d2;
                if (cd < mind) {
                    mind = cd;
                    ind = cd == d1 ? inReadPos : (-inReadPos - 1);
                    if (mind == 0) break;
                }
                ++inReadPos;
            }
            inReadPos = ind;
            //
            if (mind > 0) {
                ++Kmer2ReadIndexBuilder.uncorrected;
                continue;
            }
            //
            boolean fw = inReadPos >= 0;
            if (!fw) {
                inReadPos = -inReadPos - 1;
                inReadPos = dnaqLen - inReadPos - len;
                reverseComplement(dnaqBytes);
            }
            if ((mind > 0) && ((inReadPos == 0) || (inReadPos == dnaqLen - len + 1))) {
                continue;
            }
            dnaqs.add(dnaqBytes);
            starts.add(-inReadPos);
            poses.add(pos);
            fws.add(fw);
            lengths.add(dnaqLen);
            if (dnaqs.size() > 500) {
                return;
            }
        }
        if (dnaqs.isEmpty()) {
            return;
        }
        /*
        verbose = dnaqs.size() > 50;
        if (verbose) {
            System.err.println("size = " + dnaqs.size());
            System.err.println("kmer = " + KmerUtils.kmer2String(kmer, len));
            for (int i = 0; i < dnaqs.size(); ++i) {
                System.err.println(new DnaQ(dnaqs.get(i)));
            }
            while (true);
        }
        */
        processKmer(dnaqs, starts, err, len, verbose);
        for (int j = 0; j < dnaqs.size(); ++j) {
            ++Kmer2ReadIndexBuilder.corrected;
            if (!fws.get(j)) {
                byte[] t = dnaqs.get(j);
                reverseComplement(t);
            }
            //
            long pos = poses.get(j);
            mf.writeDnaQ(pos, new DnaQ(dnaqs.get(j)));
        }

        //System.err.println("---------------------");
        //System.err.println("calc: " + (System.currentTimeMillis() - ct));

    }

    public void processKmer(List<byte[]> dnaqs, IntList starts, int md, int len, boolean verbose) {
        int size = dnaqs.size();
        List<ByteList> al = new ArrayList<ByteList>();
        List<ByteList> ar = new ArrayList<ByteList>();
        int maxShift = 0;
        for (int i = 0; i < size; ++i) {
            byte[] b = dnaqs.get(i);
            int s = starts.get(i);
            int f = s + b.length;

            ByteList left = new ByteArrayList();
            for (int j = -1; j >= s; --j) {
                left.add(b[-s + j]);
            }
            al.add(left);

            ByteList right = new ByteArrayList();
            for (int j = len; j < f; ++j) {
                right.add(b[-s + j]);
            }
            ar.add(right);

            if (-s > maxShift) {
                maxShift = -s;
            }

        }


        //
        if (verbose) {
            System.err.println("<before>");
            for (int i = 0; i < size; ++i) {
                int s = maxShift + starts.get(i);
                for (int j = 0; j < s; ++j) {
                    System.err.print(" ");
                }
                System.err.println(new DnaQ(dnaqs.get(i)));
            }
            System.err.println("</before>");
        }
        //

        byte[] kmer = Arrays.copyOfRange(dnaqs.get(0), -starts.get(0), -starts.get(0) + len);

        //
        if (verbose) {
            for (ByteList bl : al) {
                System.err.println(new DnaQ(bl.toByteArray()));
            }
            System.err.println("l---------------------------------------------------------");
        }
        //
        long consTime = 0;
        long ct = System.currentTimeMillis();
        List<ByteList> left = consensus(al, md, verbose);
        consTime += System.currentTimeMillis() - ct;
        //
        if (verbose) {
            for (ByteList bl : left) {
                System.err.println(new DnaQ(bl.toByteArray()));
            }
            System.err.println("l+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        }
        //
        fill(al, left, md);
        /*
        for (ByteList bl : al) {
            System.err.println(new DnaQ(bl.toByteArray()));
        }
        System.err.println("==========================================================");
        */

        /*
        if (verbose) {
            for (ByteList bl : ar) {
                System.err.println(new DnaQ(bl.toByteArray()));
            }
            System.err.println("r---------------------------------------------------------");
        }
        */
        ct = System.currentTimeMillis();
        List<ByteList> right = consensus(ar, md, false);
        consTime += System.currentTimeMillis() - ct;
        /*
        if (verbose) {
            for (ByteList bl : right) {
                System.err.println(new DnaQ(bl.toByteArray()));
            }
            System.err.println("r+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        }
        */
        fill(ar, right, md);

        //System.err.println(al.size() + " " + ar.size() + ": " + left.size() + " " + right.size() + " (" + consTime + ")");

        /*
        if (left.size() > 2) {
            for (ByteList bl : left) {
                System.err.println(new DnaQ(bl.toByteArray()));
            }
            System.err.println("-------------------------");
        }
        */

        for (int i = 0; i < size; ++i) {
            if ((al.get(i) == null) || (ar.get(i) == null)) {
                continue;
            }
            int oldStart = -starts.get(i);
            int oldLen = dnaqs.get(i).length;

            int len1 = al.get(i).size();
            int len2 = len1 + len;

            byte[] newLeft = al.get(i).toByteArray();
            byte[] newRight = ar.get(i).toByteArray();
            int newStart = al.get(i).size();
            int newLen = newLeft.length + len + newRight.length;

            byte[] b = dnaqs.get(i);

            int start = newStart - oldStart;
            if (start < 0) {
                start = 0;
            }
            if (start + b.length > newLen) {
                if (b.length <= newLen) {
                    start = newLen - b.length;
                } else {
                    b = new byte[newLen];
                    start = 0;
                    dnaqs.set(i, b);
                }
            }

            for (int j = 0; j < b.length; ++j) {
                int k = j + start;
                b[j] = (k < len1) ? newLeft[len1 - 1 - k] : (k < len2) ? kmer[k - len1] : newRight[k - len2];
            }
            starts.set(i, start - len1);
        }
        //
        if (verbose) {
            System.err.println("<after>");
            for (int i = 0; i < size; ++i) {
                int s = maxShift + starts.get(i);
                for (int j = 0; j < s; ++j) {
                    System.err.print(" ");
                }
                System.err.println(new DnaQ(dnaqs.get(i)));
            }
            System.err.println("</after>");
        }
        //
    }

    int[][] forDist = new int[200][200];

    public void fill(List<ByteList> ar, List<ByteList> ans, int md) {
        int ml1 = 0, ml2 = 0;
        for (ByteList bl : ar) {
            ml1 = Math.max(ml1, bl.size());
        }
        for (ByteList bl : ans) {
            ml2 = Math.max(ml2, bl.size());
        }
        if ((ml1 >= forDist.length) || (ml2 >= forDist[0].length)) {
            ml1 = Math.max(ml1, forDist.length);
            ml2 = Math.max(ml2, forDist[0].length);
            System.err.println("here " + ml1 + " " + ml2);
            forDist = new int[ml1 + 1][ml2 + 1];
        }

        for (ByteList from : ar) {
            int mind = Integer.MAX_VALUE;
            ByteList fin = null;
            int len = -1;
            for (ByteList to : ans) {
                int l1 = from.size();
                int l2 = to.size();
                int maxL = Math.min(l1 + md, l2);
                dist(from.toByteArray(), to.subList(0, maxL).toByteArray(), forDist);
                for (int i = l1 >= md ? l1 - md : 0; (i <= l1 + md) && (i <= l2); ++i) {
                    //int cd = dist(from.toByteArray(), to.subList(0, i).toByteArray(), forDist, mind);
                    int cd = forDist[l1][i];
                    if (cd < mind) {
                        mind = cd;
                        fin = to;
                        len = i;
                    }
                }
            }
            if (fin == null) {
                continue;
            }
            if (from.size() != len) {
                from.size(len);
            }
            for (int i = 0; i < len; ++i) {
                from.set(i, fin.get(i));
            }
        }
    }

    int msize = 500;
    int mlen = 300;
    int[][][] dists = new int[msize][mlen][mlen];

    public List<ByteList> consensus(List<ByteList> ar, int md, boolean verbose) {

        int size = ar.size();

        if (size <= 2) {
            return ar;
        }

        boolean[] masked = new boolean[size];
        int maxLen = 0;
        for (int i = 0; i < size; ++i) {
            ByteList bl = ar.get(i);
            maxLen = maxLen < bl.size() ? bl.size() : maxLen;
            masked[i] = bl.size() < 4;
        }
        if (size > dists.length) {
            msize = size + 50;
        }
        if (maxLen + md >= dists[0].length) {
            mlen = maxLen + md + 50;
        }
        if ((dists.length < msize) || (dists[0][0].length < mlen)) {
            dists = new int[msize][mlen][mlen];
        }

        for (int i = 0; i < size; ++i) {
            for (int j = 0; j <= maxLen; ++j) {
                dists[i][j][0] = j;
            }
            for (int j = 0; j <= maxLen + md; ++j) {
                dists[i][0][j] = j;
            }
        }
        ByteList ans = new ByteArrayList();
        List<ByteList> rem = new ArrayList<ByteList>();

        /*
        IntList lastLengths = new IntArrayList();
        for (int i = 0; i < size; ++i) {
            lastLengths.add(0);
        }
        */
        List<IntList> lastLengths = new ArrayList<IntList>();
        for (int i = 0; i < size; ++i) {
            lastLengths.add(new IntArrayList());
            lastLengths.get(i).add(0);
        }

        for (int i = 1; i <= maxLen; ++i) {
            /*
            for (int j = 0; j < size; ++j) {
                System.err.print("(" + j + ":");
                for (int x : lastLengths.get(j)) {
                    System.err.print(" " + x);
                }
                System.err.print(") ");
            }
            System.err.println();
            */
            ans.add((byte)0);
            byte mb = -1;
            IntSet maxIncreasedByOne = new IntOpenHashSet();
            int remNumber = 0;
            for (byte s = 0; s < 4; ++s) {
                ans.set(i - 1, s);
                IntSet increasedByOne = new IntOpenHashSet();
                remNumber = 0;
                for (int j = 0; j < size; ++j) {
                    ByteList cbl = ar.get(j);
                    int cl = cbl.size();
                    if ((cl < i) || masked[j]) {
                        continue;
                    }
                    ++remNumber;
                    for (int k = 1; k <= cl; ++k) {
                        dists[j][k][i] = dists[j][k - 1][i - 1] + byteDist[ar.get(j).get(k - 1)][s];
                        if (dists[j][k][i] > dists[j][k - 1][i] + 1) {
                            dists[j][k][i] = dists[j][k - 1][i] + 1;
                        }
                        if (dists[j][k][i] > dists[j][k][i - 1] + 1) {
                            dists[j][k][i] = dists[j][k][i - 1] + 1;
                        }
                    }
                    int min = Integer.MAX_VALUE;
                    /*
                    int minLength = -1;
                    */
                    IntList minLengths = new IntArrayList();
                    for (int k = 0 < i - md + 1 ? i - md + 1 : 0; (k <= cl) && (k <= i + md - 1); ++k) {
                        int cd = dists[j][k][i];
                        if (min >= cd) {
                            if (min > cd) {
                                minLengths.clear();
                            }
                            min = cd;
                            minLengths.add(k);
                        }
                    }
                    for (int x : minLengths) {
                        boolean br = false;
                        for (int y : lastLengths.get(j)) {
                            if ((x - y == 1) && (dists[j][x][i] == dists[j][y][i - 1])) {
                                increasedByOne.add(j);
                                br = true;
                                break;
                            }
                        }
                        if (br) {
                            break;
                        }
                    }
                    /*
                    if (minLength - lastLengths.get(j) == 1) {
                        increasedByOne.add(j);
                    }
                    */
                }
                if (increasedByOne.size() > maxIncreasedByOne.size()) {
                    mb = s;
                    maxIncreasedByOne = increasedByOne;
                }
                /*
                if (verbose) {
                    System.err.print("s = " + s + "; inc = " + increasedByOne.size() + ": ");
                    for (int x : increasedByOne) {
                        System.err.print(x + " ");
                    }
                    System.err.println();
                    for (int j = 0; j < size; ++j) {
                        if (!masked[j]) {
                            System.err.print(j + ": ");
                            for (int x : lastLengths.get(j)) {
                                System.err.print("(" + x + ", " + dists[j][x][i - 1] + ") ");
                            }
                            System.err.println();
                        }
                    }
                }
                */
            }
            if ((maxIncreasedByOne.size() < remNumber * 2 / 3) || (mb == -1)) {
                List<ByteList> t = new ArrayList<ByteList>();

                List<ByteList> l1 = new ArrayList<ByteList>();
                List<ByteList> l2 = new ArrayList<ByteList>();
                if (!maxIncreasedByOne.isEmpty()) {
                    for (int j = 0; j < size; ++j) {
                        if (maxIncreasedByOne.contains(j) || (!masked[j] && (ar.get(j).size() < i))) {
                            l1.add(ar.get(j));
                        } else {
                            l2.add(ar.get(j));
                        }
                    }
                    l1 = consensus(l1, md, verbose);
                    l2 = consensus(l2, md, verbose);
                }
                //t.addAll(consensus(rem, md, verbose));
                t.addAll(l1);
                t.addAll(l2);

                ans.remove(i - 1);
                t.add(ans);

                return t;
            }
            ans.set(i - 1, mb);

            int s = mb;
            for (int j = 0; j < size; ++j) {
                ByteList cbl = ar.get(j);
                int cl = cbl.size();
                if ((cl < i) || masked[j]) {
                    continue;
                }
                for (int k = 1; k <= cl; ++k) {
                    dists[j][k][i] = dists[j][k - 1][i - 1] + byteDist[ar.get(j).get(k - 1)][s];
                    if (dists[j][k][i] > dists[j][k - 1][i] + 1) {
                        dists[j][k][i] = dists[j][k - 1][i] + 1;
                    }
                    if (dists[j][k][i] > dists[j][k][i - 1] + 1) {
                        dists[j][k][i] = dists[j][k][i - 1] + 1;
                    }
                }
                int min = Integer.MAX_VALUE;
                for (int k = 0 < i - md + 1 ? i - md + 1 : 0; (k <= cl) && (k <= i + md - 1); ++k) {
                    if ((min >= dists[j][k][i]) && ((dists[j][k][i] < k / 3) || (k < 6))) {
                        if (min > dists[j][k][i]) {
                            lastLengths.get(j).clear();
                        }
                        lastLengths.get(j).add(k);
                        min = dists[j][k][i];
                    }
                }
                //if (min > md) {
                if (min == Integer.MAX_VALUE) {
                    masked[j] = true;
                    rem.add(ar.get(j));
                    if (rem.size() >= ar.size() - 1) {
                        return ar;
                    }
                }
            }
        }
        /*
        if (verbose) {
            System.err.println("ans:");
            System.err.println(new DnaQ(ans.toByteArray()));
            System.err.println("rem:");
            for (ByteList bl : rem) {
                System.err.println(new DnaQ(bl.toByteArray()));
            }
            System.err.println("end");
        }
        */
        List<ByteList> result = new ArrayList<ByteList>();
        result.add(ans);
        if (!rem.isEmpty()) {
            result.addAll(consensus(rem, md, verbose));
        }
        if (verbose) {
            /*
            System.err.println("input:");
            for (ByteList bl : ar) {
                System.err.println(new DnaQ(bl.toByteArray()));
            }
            System.err.println("result:");
            for (ByteList bl : result) {
                System.err.println(new DnaQ(bl.toByteArray()));
            }
            System.err.println("+++++++++++++++++++++++");
            */
        }
        return result;
    }

    static int[][] byteDist = new int[][]{
            {0, 1, 1, 1},
            {1, 0, 1, 1},
            {1, 1, 0, 1},
            {1, 1, 1, 0}
    };

    public int dist(long a, long b, int len) {
        return dist(a, b, len, len);
    }

    public int dist(long a, long b, int len, int md) {
        byte[] ar = new byte[len];
        byte[] br = new byte[len];
        for (int i = 0; i < len; ++i) {
            ar[i] = (byte)((a >> (2 * (len - 1 - i))) & 3);
            br[i] = (byte)((b >> (2 * (len - 1 - i))) & 3);
        }

        int[][] dist = new int[len + 1][len + 1];
        return dist(ar, br, dist, md);
    }

    public int dist(byte[] a, int posA, byte[] b, int posB) {
        //System.err.println(posA + " " + a.length);
        //System.err.println(posB + " " + b.length);
        int beg = posA > posB ? posA : posB;
        int end = (posA + a.length) < (posB + b.length) ? (posA + a.length) : (posB + b.length);
        byte[] na = Arrays.copyOfRange(a, beg - posA, end - posA);
        byte[] nb = Arrays.copyOfRange(b, beg - posB, end - posB);
        return dist(na, nb, null);
    }

    public int dist(byte[] a, byte[] b) {
        return dist(a, b, null);
    }

    public int dist(byte[] a, byte[] b, int[][] dist) {
        return dist(a, b, dist, a.length + b.length + 1);
    }

    public int dist(byte[] a, byte[] b, int[][] dist, int md) {
        int l1 = a.length;
        int l2 = b.length;

        if (dist == null) {
            dist = new int[l1+1][l2+1];
        }
        for (int i = 0; i <= l1; ++i) {
            dist[i][0] = i;
        }
        for (int i = 0; i <= l2; ++i) {
            dist[0][i] = i;
        }
        int l = l1 + l2;
        for (int ij = 2; ij <= l; ++ij) {
            int cmd = Integer.MAX_VALUE;
            for (int i = 1 < ij - l2 ? ij - l2 : 1; (i <= l1) && (i < ij); ++i) {
                int j = ij - i;
                dist[i][j] = dist[i - 1][j - 1] + byteDist[a[i - 1]][b[j - 1]];
                if (dist[i][j] > dist[i - 1][j] + 1) {
                    dist[i][j] = dist[i - 1][j] + 1;
                }
                if (dist[i][j] > dist[i][j - 1] + 1) {
                    dist[i][j] = dist[i][j - 1] + 1;
                }
                if (dist[i][j] < cmd) {
                    cmd = dist[i][j];
                }
            }
            if ((cmd > md) && (cmd < Integer.MAX_VALUE)) {
                return cmd;
            }
        }
        return dist[l1][l2];
    }

    private static void reverseComplement(byte[] ar) {
        int m = (ar.length + 1) / 2;
        for (int k = 0; k < m; ++k) {
            byte b1 = (byte)(3 - ar[k]);
            byte b2 = (byte)(3 - ar[ar.length - 1 - k]);
            ar[k] = b2;
            ar[ar.length - 1 - k] = b1;
        }
    }

    private static byte[] readDnaQ(MultiFile2MemoryMap mf, long pos) throws IOException {
        int dnaqLen = mf.readInt(pos);
        byte[] dnaqBytes = new byte[dnaqLen];
        mf.read(pos + 4, dnaqBytes);
        for (int j = 0; j < dnaqLen; ++j) {
            dnaqBytes[j] &= 3;
        }
        return dnaqBytes;
    }


}
