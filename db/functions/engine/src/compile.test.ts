import { CompilePattern, CompilePatterns, Match1 } from './compile';
import { Args, User } from "./types";

import {expect, test} from '@jest/globals';


const JoeBob = {
    name: "JoeBob",
    roles: [],
    channels: []
} as unknown as User;

const Admin = {
} as unknown as User;

const Rolo = {
    name: "rolo",
    roles: ["lackey", "goofball"],
    channels: ["Netflix", "CNN"]
} as unknown as User;

const SomeArgs : Args = {
    "foo": "bar",
    "baz": 99,
};

test('non-pattern', () => {
    let p = CompilePattern("foobar");
    expect(p.expand(JoeBob)).toBe("foobar");
    expect(p.expand(Rolo, SomeArgs)).toBe("foobar");
});

test('no-op pattern', () => {
    let p = CompilePattern("My $$2 bill");
    expect(p.expand(JoeBob)).toBe("My $2 bill");
    expect(p.expand(Rolo, SomeArgs)).toBe("My $2 bill");
});

test('arg pattern', () => {
    let p = CompilePattern("Hey $foo!");
    expect(p.expand(JoeBob, SomeArgs)).toBe("Hey bar!");
    p = CompilePattern("From $foo to $baz");
    expect(p.expand(JoeBob, SomeArgs)).toBe("From bar to 99");
    p = CompilePattern("From$(foo)to$(baz)");
    expect(p.expand(JoeBob, SomeArgs)).toBe("Frombarto99");
});

test('user pattern', () => {
    let p = CompilePattern("some-$(user.name)");
    expect(p.expand(JoeBob, SomeArgs)).toBe("some-JoeBob");
    p = CompilePattern("some-$(context.user.name)");
    expect(p.expand(JoeBob, SomeArgs)).toBe("some-JoeBob");
});

test('user pattern with admin', () => {
    let p = CompilePattern("some-$(user.name)");
    expect(p.expand(Admin, SomeArgs)).toBe("some-");
    p = CompilePattern("some-$(context.user.name)");
    expect(p.expand(Admin, SomeArgs)).toBe("some-");
});

test('matching', () => {
    expect(CompilePatterns(undefined)).toBe(undefined);

    let pp = CompilePatterns(["foobar", "some-$(user.name)"])!;
    expect(pp.length).toBe(2);
    expect(pp[0].expand(Rolo, SomeArgs)).toBe("foobar");

    expect(Match1(pp, "foobar", Rolo, SomeArgs)).toBeTruthy();
    expect(Match1(pp, "some-rolo", Rolo, SomeArgs)).toBeTruthy();
    expect(Match1(pp, "duh", Rolo, SomeArgs)).toBeFalsy();
});