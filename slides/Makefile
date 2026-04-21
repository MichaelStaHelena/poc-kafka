MARP = npx --yes @marp-team/marp-cli@latest

.PHONY: pdf html serve clean

pdf:
	$(MARP) deck.md --pdf --allow-local-files

html:
	$(MARP) deck.md --html --allow-local-files

serve:
	$(MARP) -s .

clean:
	rm -f deck.pdf deck.html
