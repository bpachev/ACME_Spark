all:
	pdflatex spark1.tex
	bibtex spark1.aux ||true
	pdflatex spark1.tex
	pdflatex spark1.tex


