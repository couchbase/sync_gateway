import { AllowConfig, Args, Context, Credentials, Database, Document, FunctionsConfig, GraphQLConfig, CRUD, User, JSFn, HTTPError, JSONObject, FunctionConfig } from './types'
import { CompileEntityReferenceResolver, CompileFn, CompileResolver, CompileTypeNameResolver } from './compile'

import * as gq from 'graphql';
import { buildSubgraphSchema } from '@apollo/subgraph';
import { GraphQLResolverMap } from "@apollo/subgraph/dist/schema-helper";


/** Abstract interface to the N1QL query and CRUD implementation. Used by Database. */
export interface Upstream {
    query(fnName: string,
          n1ql: string,
          args: Args | undefined,
          user: User) : JSONObject[];
    get(docID: string, collection: string, user: User) : Document | null;
    save(doc: Document, docID: string | undefined, collection: string, user: User) : string | null;
    delete(docID: string, revID: string | undefined, collection: string, user: User) : boolean;
}


//////// DATABASE IMPLEMENTATION


// https://www.apollographql.com/docs/federation/building-supergraphs/subgraphs-apollo-server/
const kFederationImportsStr = `
    extend schema @link(
        url: "https://specs.apollo.dev/federation/v2.0",
        import: ["@extends", "@external", "@inaccessible", "@key", "@override", "@provides",
                 "@requires", "@shareable", "@tag"]
    )
`;


/** Constructs a Database instance. */
export function MakeDatabase(functions: FunctionsConfig | undefined,
                             graphql: GraphQLConfig | undefined,
                             upstream: Upstream) : [Database | null, string[] | null]
{
    let db : Database | null;
    db = new DatabaseImpl(upstream);
    let errors = db.configure(functions, graphql);
    if (errors) db = null;
    return [db, errors];
}


class DatabaseImpl implements Database {

    constructor(private upstream: Upstream) {
        // Create a context for "context.admin"
        let superUser = new UserImpl(this, null, true);
        this.superUserContext = new ContextImpl(superUser, superUser, true);
        superUser.context = this.superUserContext;
    }

    configure(functions: FunctionsConfig | undefined,
              graphql: GraphQLConfig | undefined) : string[] | null
    {
        // Collect all errors/exceptions in an array to return at the end:
        let errors = new ErrorList;
        console.log("Initializing GraphQL/functions...");

        if (functions) {
            let nFuncs = 0;
            let maxSize = functions.max_code_size;
            for (let fnName of Object.getOwnPropertyNames(functions.definitions)) {
                let fnConfig = functions.definitions[fnName];
                if (maxSize !== undefined && fnConfig.code.length > maxSize) {
                    errors.complain(`function ${fnName}: code is too large (> ${maxSize} bytes)`)
                } else {
                    errors.try(`function ${fnName}: `, () => {
                        this.functions[fnName] = CompileFn(fnName, fnConfig, this);
                        ++nFuncs;
                    });
                }
            }
            if (functions.max_function_count !== undefined && nFuncs > functions.max_function_count) {
                errors.complain(`too many functions (> ${functions!.max_function_count})`);
            }
        }

        if (graphql) {
            if (!graphql.schema) {
                errors.complain("GraphQL schema is missing");
            } else if (graphql.max_schema_size !== undefined && graphql.schema.length > graphql.max_schema_size) {
                errors.complain(`GraphQL schema too large (> ${graphql.max_schema_size} bytes)`);
            } else {
                errors.try(`GraphQL schema: `,  () => {
                    if (graphql.subgraph) {
                        // Prepend the required "extend schema..." declaration:
                        let document = gq.parse(kFederationImportsStr + "\n\n" + graphql.schema!);
                        // Create a map with just the __resolveReference resolver fns, so the
                        // subgraph code can store them in the schema:
                        let resolvers = this.createApolloResolverMap(graphql,errors);
                        this.schema = buildSubgraphSchema({ typeDefs: document,
                                                            resolvers: resolvers });
                    } else {
                        this.schema = gq.buildSchema(graphql.schema!);
                    }
                });
                if (this.schema) {
                    this.configureResolvers(graphql, errors);
                    if (errors.errors.length == 0)
                        this.verifyResolversExist(errors);
                }
            }
        }

        if (errors.errors.length > 0) {
            console.error(`Found ${errors.errors.length} error[s] in configuration!`);
            return errors.errors;
        }
        return null;
    }


    private configureResolvers(graphql: GraphQLConfig, errors: ErrorList) {
        let remainingResolvers = graphql.max_resolver_count ?? 1e9;

        if (!graphql.resolvers)  return;

        function canAddResolver(typeName: string, fieldName: string, config: FunctionConfig) {
            if (remainingResolvers-- == 0) {
                errors.complain(`too many GraphQL resolvers (> ${graphql.max_resolver_count!})`);
                return false;
            }
            let maxSize = graphql!.max_code_size;
            if (maxSize !== undefined && config.code.length > maxSize) {
                errors.complain(`GraphQL resolver ${typeName}.${fieldName}: code is too large (> ${maxSize} bytes)`);
                return false;
            }
            return true;
        }

        for (let typeName of Object.getOwnPropertyNames(graphql.resolvers)) {
            let fields = graphql.resolvers[typeName];
            let schemaType = this.schema!.getType(typeName);
            if (!schemaType) {
                errors.complain(`GraphQL resolver type '${typeName}': no such type in the schema`);
            } else if (schemaType instanceof gq.GraphQLObjectType) {
                let schemaFields = schemaType.getFields();
                for (let fieldName of Object.getOwnPropertyNames(fields)) {
                    let fnConfig = fields[fieldName];
                    if (canAddResolver(typeName, fieldName, fnConfig)) {
                        if (graphql.subgraph && fieldName == '__resolveReference')  continue;
                        let schemaField = schemaFields[fieldName];
                        if (schemaField) {
                            errors.try(`GraphQL resolver ${typeName}.${fieldName}: `,
                                      () => {
                                CompileResolver(schemaField, typeName, fieldName, fnConfig, this);
                                console.debug(`Compiled GraphQL resolver ${typeName}.${fieldName}`)
                            });
                        } else {
                            errors.complain(`GraphQL resolver ${typeName}.${fieldName}: no such field in the schema`);
                        }
                    }
                }
            } else if (schemaType instanceof gq.GraphQLInterfaceType
                            || schemaType instanceof gq.GraphQLUnionType) {
                let ifType = schemaType;
                for (let fieldName of Object.getOwnPropertyNames(fields)) {
                    let fnConfig = fields[fieldName];
                    if (canAddResolver(typeName, fieldName, fnConfig)) {
                        if (fieldName == "__typename") {
                            errors.try(`GraphQL resolver ${typeName}.__typename: `,
                                       () => {
                                ifType.resolveType = CompileTypeNameResolver(typeName,
                                                                             fnConfig, this);
                                console.debug(`Compiled GraphQL resolver ${typeName}.__typename`)
                            });
                        } else {
                            errors.complain(`GraphQL resolver ${typeName}.${fieldName}: abstract types may only have a '__typename' resolver`);
                        }
                    }
                }
            } else {
                errors.complain(`GraphQL type ${typeName}: not an object or interface, so cannot have resolvers`);
            }
        }
    }

    private verifyResolversExist(errors: ErrorList) {
        let queryType = this.schema!.getQueryType();
        let mutationType = this.schema!.getMutationType();

        const typeMap = this.schema!.getTypeMap()
        for (const typeName of Object.getOwnPropertyNames(typeMap)) {
            const schemaType = typeMap[typeName];
            if (schemaType == queryType || schemaType == mutationType) {
                const fields = schemaType.getFields();
                for (let fieldName of Object.getOwnPropertyNames(fields)) {
                    if (!fields[fieldName].resolve)
                        errors.complain(`GraphQL resolver ${typeName}.${fieldName}: missing function definition`);
                }
            } else if (schemaType instanceof gq.GraphQLInterfaceType
                            || schemaType instanceof gq.GraphQLUnionType) {
                if (!schemaType.resolveType)
                    errors.complain(`GraphQL resolver ${typeName}.__typename: missing function definition`);
            }
        }
    }

    createApolloResolverMap(graphql: GraphQLConfig, errors: ErrorList) : GraphQLResolverMap<unknown> {
        let result : GraphQLResolverMap<unknown> = {}
        for (let [typeName, resolvers] of Object.entries(graphql.resolvers)) {
            let resolveRef = resolvers['__resolveReference'];
            if (resolveRef) {
                let resolverFn = CompileEntityReferenceResolver(typeName, resolveRef, this);
                result[typeName] = {
                    '__resolveReference': resolverFn as gq.GraphQLFieldResolver<any,unknown>
                };
            }
        }
        return result;
    }


    makeContext(credentials: Credentials | null, mutationAllowed: boolean) {
        let user = new UserImpl(this, credentials);
        let ctx = new ContextImpl(user, this.superUserContext.user, mutationAllowed);
        user.context = ctx;
        return ctx;
    }


    getFunction(name: string) : JSFn {
        let fn = this.functions[name];
        if (!fn) throw new HTTPError(404, `No such function ${name}`);
        return fn;
    }


    callFunction(context: Context,
                 name: string,
                 args: Args | undefined)
    {
        console.debug(`>>> FUNCTION ${name}`);
        return this.getFunction(name)(context, args);
    }



    query(context: Context,
          fnName: string,
          n1ql: string,
          args: Args | undefined)    // note: `args` are top-level N1QL args, not the "args" object
          : JSONObject[]
    {
        console.debug(`>>> N1QL ${n1ql}`);
        return this.upstream.query(fnName, n1ql, args, context.user);
    }


    graphql(context: Context,
            query: string,
            variableValues?: Args,
            operationName?: string) : Promise<gq.ExecutionResult> {
        console.debug(`>>> GRAPHQL ${query}`);
        if (!this.schema) throw new HTTPError(404, "No GraphQL schema");
        return gq.graphql({
            contextValue: context,
            schema: this.schema,
            source: query,
            variableValues: variableValues,
            operationName: operationName,
        });
    }


    get(docID: string, collection: string, user: User) : Document | null {
        return this.upstream.get(docID, collection, user);
    }

    save(doc: Document, docID: string | undefined, collection: string, user: User) : string | null {
        return this.upstream.save(doc, docID, collection, user);
    }

    delete(docID: string, revID: string | undefined, collection: string, user: User) : boolean {
        return this.upstream.delete(docID, revID, collection, user);
    }


    private superUserContext: ContextImpl;       // The admin Context (only one is needed)
    private functions: Record<string,JSFn> = {}; // Compiled JS functions
    private schema?: gq.GraphQLSchema;           // Compiled GraphQL schema (with resolvers)
}


//////// CONTEXT IMPLEMENTATION


class ContextImpl implements Context {
    constructor(readonly user: User,
                readonly admin: User,
                mutationAllowed: boolean) {
        if (!mutationAllowed) {
            this.readOnlyLevel++;
        }
    }

    checkUser(name: string | string[]) : boolean {
        return this.user.isAdmin || match(name, this.user.name!);
    }

    requireUser(name: string | string[]) {
        if (!this.checkUser(name)) this.permissionDenied("user");
    }

    checkAdmin() : boolean {
        return this.user.isAdmin;
    }

    requireAdmin() {
        if (!this.checkAdmin()) this.permissionDenied("admin only");
    }

    checkRole(role: string | string[]) : boolean {
        if (this.user.isAdmin) return true;
        for (let myRole of this.user.roles!) {
            if (match(role, myRole))  return true;
        }
        return false;
    }

    requireRole(role: string | string[]) {
        if (!this.checkRole(role)) this.permissionDenied("role");
    }

    checkAccess(channel: string | string[]) : boolean {
        if (this.user.isAdmin) return true;
        for (let myChannel of this.user.channels!) {
            if (match(channel, myChannel))  return true;
        }
        return false;
    }

    requireAccess(channel: string | string[]) {
        if (!this.checkAccess(channel)) this.permissionDenied("channel");
    }

    checkAllowed(allow: AllowConfig | undefined) : boolean {
        return this.user.isAdmin
            || (allow !== undefined && (
                    (allow.users !== undefined    && allow.users.includes(this.user.name!)) ||
                    (allow.roles !== undefined    && this.checkRole(allow.roles)) ||
                    (allow.channels !== undefined && this.checkAccess(allow.channels))));
    }

    requireAllowed(allow: AllowConfig | undefined) {
        if (!this.checkAllowed(allow)) this.permissionDenied();
    }

    checkMutating() : boolean {
        return this.readOnlyLevel == 0;
    }

    requireMutating() : void {
        if (!this.checkMutating()) throw new HTTPError(403, "Permission denied (read-only context)");
    }

    private permissionDenied(message: string = "") : never {
        if (message) message = ` (${message})`;
        if (this.user.isGuest) {
            throw new HTTPError(401, "Login required" + message);
        } else {
            throw new HTTPError(403, "Permission denied" + message);
        }
    }

    readOnlyLevel = 0;
}

export function BeginReadOnly(context: Context) {
    if (!context.user.isSuperUser) {
        (context as ContextImpl).readOnlyLevel++;
    }
    console.debug(`++++ BeginReadOnly; now ${(context as ContextImpl).readOnlyLevel}`);
}

export function EndReadOnly(context: Context) {
    if (!context.user.isSuperUser) {
        (context as ContextImpl).readOnlyLevel--;
    }
    console.debug(`---- EndReadOnly; now ${(context as ContextImpl).readOnlyLevel}`);
}


//////// CRUD IMPLEMENTATION


class CRUDImpl implements CRUD {

    constructor(private db: DatabaseImpl,
                private collection: string,
                private user: UserImpl) { }


    get(docID: string) : Document | null {
        return this.db.get(docID, this.collection, this.user);
    }


    save(doc: Document, docID?: string) : string | null {
        if (!this.user.canMutate)
            throw new HTTPError(403, "save() is not allowed in a read-only context");
        return this.db.save(doc, docID, this.collection, this.user);
    }


    delete(docOrID: string | Document) : boolean {
        if (!this.user.canMutate)
            throw new HTTPError(403, "delete() is not allowed in a read-only context");
        if (typeof docOrID === 'string') {
            return this.db.delete(docOrID, undefined, this.collection, this.user);
        } else {
            let id = docOrID['_id'];
            if (!id) throw "delete() called with doc object that has no '_id' property";
            return this.db.delete(id, docOrID._rev, this.collection, this.user);
        }
    }
}


//////// USER IMPLEMENTATION


export let CallDepth = 1;
export const MaxCallDepth = 20;

export function ClearCallDepth() {CallDepth = 1;}


class UserImpl implements User {

    constructor(private db: DatabaseImpl,
                credentials: Credentials | null,
                readonly isSuperUser = false)
    {
        if (credentials) {
            [this.name, this.roles, this.channels] = credentials;
            isSuperUser = false;
        }
        this.defaultCollection = new CRUDImpl(db, '_default', this);
    }


    // Authorization:

    readonly name?: string;
    readonly roles?: string[];
    readonly channels?: string[];

    get isGuest() {return this.name === "";}

    get isAdmin() {return this.name === undefined;}

    get canMutate() : boolean {return this.isSuperUser || this.context.checkMutating();}


    // API:

    readonly defaultCollection: CRUD;

    collection(name: string) : CRUD {
        return new CRUDImpl(this.db, name, this);
    }

    function(name: string, args?: Args) : unknown {
        let fn = this.db.getFunction(name);
        if (++CallDepth > MaxCallDepth) {
            --CallDepth;
            let msg = `User function recursion too deep (calling function("${name}")`;
            console.error(msg);
            throw new HTTPError(508, msg);
        }
        try {
            return fn(this.context, args);
        } finally {
            --CallDepth;
        }
    }


    async graphql(query: string, args?: Args) : Promise<JSONObject | null> {
        if (++CallDepth > MaxCallDepth) {
            --CallDepth;
            let msg = `User function recursion too deep (calling graphql())`;
            console.error(msg);
            throw new HTTPError(508, msg);
        }
        try {
            let result = await this.db.graphql(this.context, query, args);
            if (result.errors) {
                let err = result.errors[0];
                if (err.originalError)
                    throw err.originalError;
                throw Error(err.message);
            }
            if (result.data === undefined) return null;
            return result.data;
        } finally {
            --CallDepth;
        }
    }


    toJSON(key: string) : any {
        if (this.isAdmin) {
            return {};
        } else {
            return {
                name: this.name,
                roles: this.roles,
                channels: this.channels
            };
        }
    }

    context!: ContextImpl;
};


//////// UTILITIES:


/** Utility that collects a list of error messages. */
class ErrorList {
    /** adds an error message to `errors`. */
    complain(msg: string) {
        console.error(msg);
        this.errors.push(msg);
    }
    /** calls a function, catching any exception and adding it to `errors`. */
    try(msg: string, fn: ()=>void) {
        try {
            fn();
        } catch (err) {
            if (err instanceof Error) {
                msg += err.message;
            } else {
                msg += String(err);
            }
            this.complain(msg);
        }
    };

    errors: string[] = [];
}


// Returns true if `what` is equal to `against` or included in it.
function match(what: string | string[], against: string) {
    if (typeof(what) === 'string') {
       return (what == against);
    } else {
       return what.includes(against);
   }
}
