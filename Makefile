swixt: swixt.c
	clang -I/opt/homebrew/include/postgresql@14 -L/opt/homebrew/lib/postgresql@14 -lpq -o swixt swixt.c
