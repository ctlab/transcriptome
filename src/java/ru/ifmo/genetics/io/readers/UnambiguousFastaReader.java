package ru.ifmo.genetics.io.readers;

import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.dna.IDna;

import java.io.*;

public class UnambiguousFastaReader {
	BufferedReader br;
	
	public UnambiguousFastaReader(File f) throws FileNotFoundException {
		br = new BufferedReader(new FileReader(f));
	}
	
	public Dna read() throws IOException {
		String s;
		while (true) {
			s = br.readLine();
			if (s == null) {
				return null;
			}
			if (isComment(s)) {
				continue;
			}
			break;
		}
		StringBuilder sb = new StringBuilder();
		while (true) {
			sb.append(s);
			s = br.readLine();
			if (s == null || isComment(s)) {
				break;
			}
		}
		return new Dna(sb.toString());
	}

	private boolean isComment(String s) {
		return s.startsWith(">") || s.startsWith(";");
	}
}
