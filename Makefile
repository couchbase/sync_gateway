buildit:
	./build.sh
clean:
	rm -rf bin pkg
buildclean: clean buildit
cleanbuild: clean buildit
test:
	@./test.sh
