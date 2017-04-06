NAME = netsage-aggregate-flow-importer
VERSION = 1.0.0

rpm:    dist
	rpmbuild -ta dist/$(NAME)-$(VERSION).tar.gz
clean:
	rm -rf dist/$(NAME)-$(VERSION)/
	rm -rf dist
dist:
	rm -rf dist/$(NAME)-$(VERSION)/
	mkdir -p dist/$(NAME)-$(VERSION)/
	cp -r bin conf cron $(NAME).spec dist/$(NAME)-$(VERSION)/
	cd dist; tar -czvf $(NAME)-$(VERSION).tar.gz $(NAME)-$(VERSION)/ --exclude .svn
