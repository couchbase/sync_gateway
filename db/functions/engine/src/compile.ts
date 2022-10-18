import { AllowConfig, Args, Context, Database, FunctionConfig, User, JSFn, ResolverFn, HTTPError, ResolveInfo } from './types'

import * as gq from 'graphql';


function nonEmpty<T>(a: T[] | undefined) : a is T[] {
    return a !== undefined && a.length > 0;
}


//////// PATTERN SUBSTITUTION (in 'allow')


const kPatternRegex = /\$(\w+|\([^)]+\)|\$)/g;

/** A string that may change based on the current user and args. */
export abstract class Pattern {
    constructor(protected str: string) { }

    abstract expand(user: User, args?: Args) : string;
}

class NonPattern extends Pattern {
    constructor(str: string) {super(str);}

    override expand(user: User, args?: Args) : string {
        return this.str;
    }
}

class DollarPattern extends Pattern {
    constructor(str: string) {super(str);}

    override expand(user: User, args?: Args) : string {
        return this.str.replace(kPatternRegex, (match, varName) => {
            if (varName === '$') {
                return varName;     // $$ -> $
            }
            if (varName[0] == '(') {
                varName = varName.slice(1, -1);
            }
            if (varName.startsWith("user.")) {
                varName = "context." + varName;
            }
            if (varName.startsWith("context.user.")) {
                if (!user) {
                    return "";
                } else if (varName === "context.user.name") {
                    return user.name ?? "";
                } else if (varName === "context.user.email") {
                    return ""; //TODO
                }
            }
            let value = args ? args[varName] : undefined;
            switch(typeof(value)) {
            case "undefined":
                throw new HTTPError(503, `Bad config: Invalid channel/role pattern '$${varName} in 'allow'`);
            case "string":
                return value;
            case "number":
                return String(value);
            default:
                throw new HTTPError(400, `Value of arg '${varName}' must be a string or number`);
            }
        });
    }
}

/** Creates a Pattern from a string. '$'-prefixed patterns will be substituted according to the
 *  rules of the 'allow' object. */
export function CompilePattern(name: string) : Pattern {
    if (name.match(kPatternRegex)) {
        return new DollarPattern(name);
    } else {
        return new NonPattern(name);
    }
}

/** Creates an array of Patterns from an array of strings, or `undefined` if there are none. */
export function CompilePatterns(names?: string[]) : Pattern[] | undefined {
    return nonEmpty(names) ? names.map( name => CompilePattern(name) ) : undefined;
}

/** Matches an array of Patterns against an array of strings, returning true if any match. */
export function Match(patterns: Pattern[], values: string[] | undefined, user: User, args?: Args) {
    return nonEmpty(values) && patterns.some( pat => values.includes(pat.expand(user, args)));
}

/** Matches an array of Patterns against a single string, returning true if any match. */
export function Match1(patterns: Pattern[], value: string, user: User, args?: Args) {
    return patterns.some( pat => (value == pat.expand(user, args)));
}


//////// AUTHORIZATION


/** Checks authorization for a user, based on an Allow object. Created by CompileAllow. */
export abstract class Allow {
    constructor(public readonly name: string) { }

    abstract authorize(user: User, args?: Args) : void;

    fail() : never {
        throw new HTTPError(403, `Access forbidden to function '${this.name}'`);
    }
}

class AllowAdminOnly extends Allow {
    override authorize(user: User, args?: Args) {
        if (!user.isAdmin) this.fail();
    }
}

class AllowAnyone extends Allow {
    override authorize(user: User) { }
}

class AllowByConfig extends Allow {
    constructor(name: string, config: AllowConfig) {
        super(name);
        this.users = CompilePatterns(config.users);
        this.roles = CompilePatterns(config.roles);
        this.channels = CompilePatterns(config.channels);
    }

    authorize(user: User, args?: Args) {
        if (!user.isAdmin
                && !(this.users    && Match1(this.users, user.name!, user, args))
                && !(this.roles    && Match(this.roles, user.roles, user, args))
                && !(this.channels && Match(this.channels, user.channels, user, args)))
            this.fail();
    }

    private users?:    Pattern[];
    private roles?:    Pattern[];
    private channels?: Pattern[];
}

/** Creates an Allow object based on an optional AllowConfig.
 *  If there is no config, the `lenient` parameter determines whether that means
 *  "anyone" or "admin only". */
export function CompileAllow(name: string, config: AllowConfig | undefined, lenient: boolean) : Allow {
    if (!config) {
        return lenient ? new AllowAnyone(name) : new AllowAdminOnly(name);
    } else if (!config.users && !config.roles && !config.channels) {
        return new AllowAdminOnly(name);
    } else if ((config.users && config.users.includes("*"))
                || (config.roles && config.roles.includes("*"))
                || (config.channels && config.channels.includes("*"))) {
        return new AllowAnyone(name);
    } else {
        return new AllowByConfig(name, config);
    }
}


//////// ARG VALIDATION:


/** A function that validates args, throwing an exception if they're invalid. */
export type ArgsValidator = (args:Args | undefined) => void;


/** Given a function's parameter list, returns a function to validate the 'args' object. */
export function CompileParams(fnName: string, parameters: string[] | undefined): ArgsValidator {
    if (!nonEmpty(parameters)) {
        return (args?: Args) : void => {
            if (args) {
                let argNames = Object.getOwnPropertyNames(args);
                if (argNames.length > 0) {
                    throw `Undeclared arguments '${argNames.join("', '")}' passed to ${fnName}`;
                }
            }
        }
    } else {
        let paramSet = new Set<string>;
        for (let param of parameters) {
            paramSet.add(param);
        }
        let nParams = paramSet.size;
        if (nParams != parameters.length) {
            throw `Function/resolver ${fnName} has duplicate arg names`;
        }
        return (args?: Args) : void => {
            if (!args) {
                throw `Function "${fnName}" called without arguments, but takes ${nParams}`;
            }
            for (let param of parameters) {
                if (args[param] === undefined) {
                    throw `Missing argument "${param}" in call to ${fnName}`;
                }
            }
            let argNames = Object.getOwnPropertyNames(args);
            if (argNames.length != nParams) {
                for (let arg of argNames) {
                    if (!paramSet.has(arg)) {
                        throw `Undeclared argument "${arg}" passed to ${fnName}`;
                    }
                }
            }
        };
    }
}


//////// COMPILING FUNCTIONS:


// Returns the N1QL query string from a FunctionConfig.
function preprocessN1QL(config: FunctionConfig) : string {
    return config.code.replace(/\$_keyspace\b/g, `_default`);
}


/** Compiles a FunctionConfig to a function. */
export function CompileFn(fnName: string,
                          fnConfig: FunctionConfig,
                          db: Database) : JSFn
{
    let allow = CompileAllow(fnName, fnConfig.allow, false);
    let checkArgs = CompileParams(fnName, fnConfig.args);
    switch (fnConfig.type) {
    case "query":
        let n1ql = preprocessN1QL(fnConfig);
        return async function(context, args) {
            checkArgs(args);
            allow.authorize(context.user, args);
            return db.query(fnName, n1ql, args, context);
        };
    case "javascript":
        let code = compileToJS(fnName, fnConfig, 2);
        return function(context, args) {
            console.log(`FUNC ${fnName}`);
            checkArgs(args);
            allow.authorize(context.user, args);
            let result = code(context, args);
            if (!(result instanceof Promise)) result = Promise.resolve(result)
            return result
        };
    default:
        throw `Function ${fnName} has an unknown or missing type`;
    }
}


/** Compiles a FunctionConfig to a GraphQL resolver. */
export function CompileResolver(typeName: string,
                                fieldName: string,
                                fnConfig: FunctionConfig,
                                db: Database) : gq.GraphQLFieldResolver<any,Context>
{
    let fnName = `${typeName}.${fieldName}`;
    let allow = CompileAllow(fnName, fnConfig.allow, true);
    if (fnConfig.args) {
        throw `GraphQL resolver ${fnName} should not have an 'args' declaration`;
    }
    switch (fnConfig.type) {
    case "query":
        let n1ql = preprocessN1QL(fnConfig);
        return async function(source, args, context, info) {
            allow.authorize(context.user, args);
            return db.query(fnName, n1ql, args, context);
        };
    case "javascript":
        let code = compileToJS(fnName, fnConfig, 4) as ResolverFn;
        return function(source, args, context, info) {
            allow.authorize(context.user, args);
            return code(source, args, context, upgradeInfo(info));
        }
    default:
        throw `Resolver function ${fnName} has an unknown or missing type`;
        }
}


/** Compiles a FunctionConfig to a GraphQL type-name (interface) resolver. */
export function CompileTypeNameResolver(typeName: string,
                                        fnConfig: FunctionConfig,
                                        db: Database) : gq.GraphQLTypeResolver<any,Context>
{
    let fnName = `${typeName}.__typename`;
    if (fnConfig.type != "javascript") {
        throw `Type-name resolver ${fnName} must be implemented in JavaScript`;
    } else if (fnConfig.allow !== undefined) {
        throw `Type-name resolver ${fnName} must not have an 'allow' config`;
    }
    return compileToJS(fnName, fnConfig, 4) as gq.GraphQLTypeResolver<any,Context>;
}


// Compiles FunctionConfig.code to JavaScript if it's a string, and checks parameter count.
function compileToJS(name: string, fnConfig: FunctionConfig, nArgs: number) : Function {
    // http://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/eval#never_use_eval!
    let fn: Function;
    try {
        fn = Function(`"use strict"; return (${fnConfig.code})`)()
    } catch (x) {
        throw `Function ${name} failed to compile: ${x}`;
    }
    if (typeof(fn) !== 'function') {
        throw `Function ${name}'s code does not compile to a JS function`;
    } else if (fn.length != nArgs) {
        throw `Function ${name} should have ${nArgs} JavaScript arguments`;
    }
    return fn;
}


function upgradeInfo(info: gq.GraphQLResolveInfo) : ResolveInfo {
    function selectedFieldNames(this: ResolveInfo) : string[] {
        let result: string[] = [];
        if (this.fieldNodes.length > 0) {
            let set = this.fieldNodes[0].selectionSet;
            if (set) {
                for (let sel of set.selections) {
                    if (sel.kind == "Field" && sel.name.kind == "Name") {
                        result.push(sel.name.value)
                    }
                }
            }
        }
        return result;
    }
    Object.defineProperty(info, "selectedFieldNames", {get: selectedFieldNames});
    return info as unknown as ResolveInfo;
}