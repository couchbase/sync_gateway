import { AllowConfig, Args, Context, Database, FunctionConfig, User, JSFn, ResolverFn, HTTPError, ResolveInfo, EntityReferenceResolver, TypeResolverFn } from './types'
import { BeginReadOnly, EndReadOnly } from "./impl";

import * as gq from 'graphql';


function nonEmpty<T>(a: T[] | undefined) : a is T[] {
    return a !== undefined && a.length > 0;
}


//////// PATTERN SUBSTITUTION (in 'allow')


const kPatternRegex = /\${([^}]+)}|(\\\$)/g;

/** A string that may change based on the current user and args. */
export abstract class Pattern {
    constructor(protected str: string) { }

    abstract expand(user: User, args?: Args) : string;

    toString() : string {
        return this.str;
    }
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
        let result = this.str.replace(kPatternRegex, (match, expr, none) => {
            if (none) {
                return '$';     // \$ -> $
            } else if (expr.startsWith("context.user.name")) {
                if (!user) {
                    return "";
                } else {
                    return user.name ?? "";
                }
            } else if (expr.startsWith("args.")) {
                let varName = expr.slice(5)
                let value = args ? args[varName] : undefined;
                switch(typeof(value)) {
                case "undefined":
                    throw new HTTPError(503, "Bad config: Unknown arg in channel/role pattern '${" + expr + "} in 'allow'");
                case "string":
                    return value;
                case "number":
                    return String(value);
                default:
                    throw new HTTPError(400, "Value of arg '" + varName + "' must be a string or number");
                }
            } else {
                throw new HTTPError(503, "Bad config: Invalid channel/role pattern '${" + expr + "} in 'allow'");
            }
        });
        return result;
    }

    override toString() : string {
        return "Pattern<" + this.str + ">";
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
        console.assert(user.defaultCollection !== undefined);
        if (!user.isAdmin
                && !(this.users    && Match1(this.users, user.name!, user, args))
                && !(this.roles    && Match(this.roles, user.roles, user, args))
                && !(this.channels && Match(this.channels, user.channels, user, args))) {
            throw new HTTPError(403, `Access forbidden to function '${this.name}' ... user = ${user.name} ... user.channels=${user.channels} ... this.users=${this.users} ... this.channels=${this.channels}`);
            //this.fail();
        }
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
                    throw new HTTPError(400, `Undeclared arguments '${argNames.join("', '")}' passed to ${fnName}`);
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
            throw new HTTPError(500, `Function/resolver ${fnName} has duplicate arg names`);
        }
        return (args?: Args) : void => {
            if (!args) {
                throw new HTTPError(400, `Function "${fnName}" called without arguments, but requires ${parameters}`);
            }
            for (let param of parameters) {
                if (args[param] === undefined) {
                    throw new HTTPError(400, `Missing argument "${param}" in call to ${fnName}`);
                }
            }
            let argNames = Object.getOwnPropertyNames(args);
            if (argNames.length != nParams) {
                for (let arg of argNames) {
                    if (!paramSet.has(arg)) {
                        throw new HTTPError(400, `Undeclared argument "${arg}" passed to ${fnName}`);
                    }
                }
            }
        };
    }
}


//////// COMPILING FUNCTIONS:


// Returns the N1QL query string from a FunctionConfig.
function checkN1QL(config: FunctionConfig) : string {
    if (!config.code.match(/^\s*\(*SELECT\b/i))
        throw new HTTPError(500, "only SELECT queries are allowed");
    return config.code;
}


// Annotates an exception with the name of the active function, and re-throws it.
function rethrow(x: unknown, what: string, fnName: string) : never {
    if (x instanceof Error) {
        x.message = `${x.message} (thrown by ${what} ${fnName})`
        throw x;
    } else {
        throw Error(`${x} (thrown by ${what} ${fnName})`);
    }
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
        let n1ql = checkN1QL(fnConfig);
        return function(context, args) {
            checkArgs(args);
            allow.authorize(context.user, args);
            console.debug(`QUERY ${fnName}`);
            if (args) args = {args: args};
            return db.query(context, fnName, n1ql, args);
        };
    case "javascript":
        let mutating = fnConfig.mutating ?? false;
        let code = compileToJS(fnName, fnConfig, 2) as JSFn;
        return function(context, args) {
            console.debug(`FUNC ${fnName}`);
            checkArgs(args);
            allow.authorize(context.user, args);
            if (!mutating) BeginReadOnly(context);
            try {
                return code(context, args);
            } catch (x) {
                rethrow(x, "function", fnName);
            } finally {
                if (!mutating) EndReadOnly(context);
            }
        };
    default:
        throw new HTTPError(500, `unknown or missing 'type'`);
    }
}


/** Compiles a FunctionConfig to a GraphQL resolver. */
export function CompileResolver(field: gq.GraphQLField<any,Context>,
                                typeName: string,
                                fieldName: string,
                                fnConfig: FunctionConfig,
                                db: Database)
{
    let fnName = `${typeName}.${fieldName}`;
    let mutating = (typeName == "Mutation");
    let allow = CompileAllow(fnName, fnConfig.allow, true);
    if (fnConfig.args) {
        throw new HTTPError(500, `should not have an 'args' declaration`);
    }
    switch (fnConfig.type) {
    case "query":
        let n1ql = checkN1QL(fnConfig);
        let fieldType = field.type;
        if (gq.isNonNullType(fieldType))
            fieldType = fieldType.ofType;
        let returnsGraphQLList = gq.isListType(fieldType);
        let returnsGraphQLScalar = gq.isScalarType(fieldType);
        field.resolve = function(parent, args, context, info) {
            allow.authorize(context.user, args);
            try {
                let result = db.query(context, fnName, n1ql, {args: args, parent: parent});
                if (returnsGraphQLList) {
                    // Resolver returns a list, so just return the array of rows
                    return result;
                } else if (result.length == 0) {
                    return null;
                } else {
                    // GraphQL result is not a list (array), but N1QL always returns an array.
                    // So use the first row of the result as the value.
                    let row = result[0];
                    if (!returnsGraphQLScalar) {
                        return row
                    } else {
                        // GraphQL result type is a scalar, but a N1QL row is always an object.
                        // Use the single field of the object, if any, as the result:
                        let cols = Object.getOwnPropertyNames(row);
                        if (cols.length == 1) {
                            return row[cols[0]];
                        } else {
                            throw new HTTPError(500, `resolver returns scalar type ${field.type}, but its N1QL query returns ${cols.length} columns, not 1`);
                        }
                    }
                }
            } catch (x) {
                rethrow(x, "resolver", fnName);
            }
        };
        break;
    case "javascript":
        let code = compileToJS(fnName, fnConfig, 4) as ResolverFn;
        field.resolve = function(source, args, context, info) {
            console.debug(`RESOLVE ${fnName}`);
            allow.authorize(context.user, args);
            if (!mutating) BeginReadOnly(context);
            try {
                return code(source, args, context, upgradeInfo(info));
            } catch (x) {
                rethrow(x, "resolver", fnName);
            } finally {
                if (!mutating) EndReadOnly(context);
            }
        };
        break;
    default:
        throw new HTTPError(500, `unknown or missing 'type'`);
    }
}


/** Compiles a FunctionConfig to a GraphQL type-name (interface) resolver. */
export function CompileTypeNameResolver(typeName: string,
                                        fnConfig: FunctionConfig,
                                        db: Database) : gq.GraphQLTypeResolver<any,Context>
{
    let fnName = `${typeName}.__typename`;
    if (fnConfig.type != "javascript") {
        throw new HTTPError(500, `type-name resolvers must be implemented in JavaScript`);
    } else if (fnConfig.allow !== undefined) {
        throw new HTTPError(500, `type-name resolver must not have an 'allow' config`);
    }
    let fn = compileToJS(fnName, fnConfig, 4) as TypeResolverFn;
    return (value, context, info) => {
        return fn(context, value, info as ResolveInfo);
    }

}


export type ApolloEntityReferenceResolver = (reference: object,
                                             context: Context,
                                             info: ResolveInfo) => any;


/** Compiles a FunctionConfig to a GraphQL Subgraph `_resolveReference` resolver. */
export function CompileEntityReferenceResolver(typeName: string,
    fnConfig: FunctionConfig,
    db: Database) : ApolloEntityReferenceResolver
{
    let fnName = `${typeName}._resolveReference`;
    if (fnConfig.type != "javascript") {
        throw new HTTPError(500, `entity reference resolvers must be implemented in JavaScript`);
    } else if (fnConfig.allow !== undefined) {
        throw new HTTPError(500, `entity reference resolver must not have an 'allow' config`);
    }
    let code = compileToJS(fnName, fnConfig, 3) as EntityReferenceResolver;
    return function(ref, context, info) {
        return code(context, ref, info);    // Apollo's library has the params swapped
    }
}


// Compiles FunctionConfig.code to JavaScript if it's a string, and checks parameter count.
function compileToJS(name: string, fnConfig: FunctionConfig, nArgs: number) : Function {
    // http://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/eval#never_use_eval!
    let fn: Function;
    try {
        fn = Function(`"use strict"; return (${fnConfig.code})`)()
    } catch (x) {
        throw new HTTPError(500, `failed to compile: ${x}`);
    }
    if (typeof(fn) !== 'function') {
        throw new HTTPError(500, `code does not compile to a JS function`);
    } else if (fn.length < 2 || fn.length > nArgs) {
        throw new HTTPError(500, `should have 2-${nArgs} JavaScript arguments`);
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
