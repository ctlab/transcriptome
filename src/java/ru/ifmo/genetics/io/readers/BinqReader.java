package ru.ifmo.genetics.io.readers;

import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.io.sources.NamedSource;
import ru.ifmo.genetics.utils.FileUtils;
import ru.ifmo.genetics.utils.iterators.ProgressableIterator;

import java.io.*;
import java.util.NoSuchElementException;

public class BinqReader implements NamedSource<DnaQ> {
	private File f;
    private long sizeBytes;
    private String libraryName;

	public BinqReader(File f) throws IOException {
		this.f = f;
        sizeBytes = FileUtils.fileSize(f);
        libraryName = FileUtils.removeExtension(f.getName(), ".binq");
	}

    public BinqReader(String s) throws IOException {
        this(new File(s));
    }

	@Override
	public MyIterator iterator() {
		try {
			return new MyIterator(new BufferedInputStream(new FileInputStream(f)));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

    @Override
    public String name() {
        return libraryName;
    }

    public class MyIterator implements ProgressableIterator<DnaQ> {
		private InputStream in;
		private DnaQ next;
        private long position = 0;

		public MyIterator(InputStream in) {
            this.in = in;
		}

		@Override
		public boolean hasNext() {
			if (next == null) {
				try {
					next = read(in);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			return next != null;
		}

		@Override
		public DnaQ next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			DnaQ res = next;
			next = null;
			return res;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

        @Override
        public double progress() {
            return (double)position / sizeBytes;
        }

        public long position() {
            return position;
        }

        private DnaQ read(InputStream in) throws IOException {
            int ch1 = in.read();
            while (ch1 == 255) {
                ch1 = in.read();
                ++position;
            }
            int ch2 = in.read();
            int ch3 = in.read();
            int ch4 = in.read();
            int len = (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0);
            if (len < 0) {
                return null;
            }
            /*
            if (len > 1000) {
                System.err.println("len = " + len + "; pos = " + position);
            }
            */
            DnaQ res = DnaQ.getPrototype(len);

            int x = 0;
            do {
                int t = in.read(res.value, x, len - x);
                if (t == -1)
                    throw new RuntimeException("Unexpected end of file");
                x += t;
            } while (x < len);
            position += len + 4;
            return res;
        }

    }
}

