package ru.ifmo.genetics.io.readers;

import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.dna.DnaQBuilder;
import ru.ifmo.genetics.dna.DnaTools;
import ru.ifmo.genetics.io.formats.QualityFormat;
import ru.ifmo.genetics.io.sources.NamedSource;
import ru.ifmo.genetics.utils.FileUtils;
import ru.ifmo.genetics.utils.iterators.ProgressableIterator;

import java.io.*;
import java.util.InputMismatchException;
import java.util.NoSuchElementException;

import static ru.ifmo.genetics.utils.TextUtils.NL;

public class FastqReader implements NamedSource<DnaQ> {
	private final File f;
	private final QualityFormat qf;
    private final String libraryName;
    private final long sizeBytes;

    public FastqReader(File f, QualityFormat qf) throws IOException {
		this.f = f;
		this.qf = qf;
        sizeBytes = FileUtils.fileSize(f);
        String name = f.getName();
        libraryName = FileUtils.removeExtension(name, ".fastq", ".fq");
	}

	@Override
	public ProgressableIterator<DnaQ> iterator() {
		try {
			return new MyIterator(new BufferedReader(new FileReader(f)));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

    @Override
    public String name() {
        return libraryName;
    }

    class MyIterator implements ProgressableIterator<DnaQ> {
		private BufferedReader br;
		private DnaQ next;
        private long position = 0;

		public MyIterator(BufferedReader br) {
			this.br = br;
			this.next = null;
		}

		@Override
		public boolean hasNext() {
			if (next == null) {
				try {
					next = read(br);
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

        private DnaQ read(BufferedReader br) throws IOException {
            String head = br.readLine();
            if (head == null) {
                br.close();
                return null;
            }
            position += head.length() + NL.length();

            char[] data = br.readLine().toCharArray();
            position += data.length + NL.length();

            String head2 = br.readLine();
            position += head2.length() + NL.length();

            char[] qual = br.readLine().toCharArray();
            position += qual.length + NL.length();

            if (data.length != qual.length) {
                throw new InputMismatchException("data.length() != qual.length()");
            }
            DnaQBuilder builder = new DnaQBuilder(data.length);
            for (int i = 0; i < data.length; i++) {
                if (data[i] == 'N' || data[i] == '.') {
                    builder.unsafeAppendUnknown();
                } else {
                    builder.unsafeAppend(DnaTools.fromChar(data[i]), qf.getPhred(qual[i]));
                }
            }
            return builder.build();
        }

    }
}
