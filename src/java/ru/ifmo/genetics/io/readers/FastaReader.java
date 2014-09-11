package ru.ifmo.genetics.io.readers;

import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.io.sources.NamedSource;
import ru.ifmo.genetics.utils.FileUtils;
import ru.ifmo.genetics.utils.iterators.ProgressableIterator;

import java.io.*;
import java.util.NoSuchElementException;

import static ru.ifmo.genetics.utils.TextUtils.NL;

// :ToDo: implement via FastaRecordReader
public class FastaReader implements NamedSource<Dna> {
	private final File f;
    private final String libraryName;
    private final long sizeBytes;

	public FastaReader(File f) throws IOException {
		this.f = f;
        sizeBytes = FileUtils.fileSize(f);
        libraryName = FileUtils.removeExtension(f.getName(), ".fn", ".fa", ".fasta");
    }

	@Override
	public ProgressableIterator<Dna> iterator() {
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

    class MyIterator implements ProgressableIterator<Dna> {
		private BufferedReader br;
		private Dna next;
        private long position;

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
		public Dna next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			Dna res = next;
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

        private Dna read(BufferedReader br) throws IOException {
            String head = br.readLine();
            if (head == null) {
                br.close();
                return null;
            }
            position += head.length() + NL.length();
            String data = br.readLine();
            Dna dna = new Dna(data);
            position += data.length() + NL.length();

            return dna;
        }

    }
}
