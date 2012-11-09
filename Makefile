compile:
	@./rebar compile

clean:
	@./rebar clean

test:
	@./rebar eunit skip_deps=true

doc:
	@./rebar doc

.PHONY: compile clean test doc
