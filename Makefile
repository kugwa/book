book: book.c
	gcc -o book book.c -ljson-c -lhiredis

clean:
	rm -f book
