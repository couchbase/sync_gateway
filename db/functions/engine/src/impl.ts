import { AllowConfig, Args, Context, Credentials, Database, Document, FunctionsConfig, GraphQLConfig, CRUD, User, JSFn, HTTPError, JSONObject } from './types'
import { CompileFn, CompileResolver, CompileTypeNameResolver } from './compile'

import * as gq from 'graphql';


/** Abstract interface to the N1QL query and CRUD implementation. Used by Database. */
export interface Upstream {
    query(fnName: string,
          n1ql: string,
          args: Args | undefined,
          user: User) : JSONObject[];
    get(docID: string, user: User) : Document | null;
    save(doc: Document, docID: string | undefined, user: User) : string | null;
    delete(docID: string, revID: string | undefined, user: User) : boolean;
}


//////// DATABASE IMPLEMENTATION


/** Constructs a Database instance. */
export function MakeDatabase(functions: FunctionsConfig | undefined,
                             graphql: GraphQLConfig | undefined,
                             upstream: Upstream) : Database
{
    return new DatabaseImpl(functions, graphql, upstream);
}


class DatabaseImpl implements Database {

    constructor(functions: FunctionsConfig | undefined,
                graphql: GraphQLConfig | undefined,
                private upstream: Upstream)
    {
        let adminUser = new UserImpl(this, null);
        this.adminContext = new ContextImpl(adminUser, adminUser, true);
        adminUser.context = this.adminContext;

        this.functions = {}
        if (functions) {
            console.debug("Compiling functions...")
            for (let fnName of Object.getOwnPropertyNames(functions.definitions)) {
                let fnConfig = functions.definitions[fnName];
                this.functions[fnName] = CompileFn(fnName, fnConfig, this);
            }
        }

        if (graphql) {
            console.debug("Compiling GraphQL schema and resolvers...")
            if (!graphql.schema) throw new HTTPError(500, "GraphQL schema is missing");
            this.schema = gq.buildSchema(graphql.schema);
            for (let typeName of Object.getOwnPropertyNames(graphql.resolvers)) {
                let fields = graphql.resolvers[typeName];
                let schemaType = this.schema.getType(typeName);
                if (!schemaType) {
                    throw new HTTPError(500, `GraphQL schema has no type '${typeName}'`);
                } else if (schemaType instanceof gq.GraphQLObjectType) {
                    let schemaFields = schemaType.getFields();
                    for (let fieldName of Object.getOwnPropertyNames(fields)) {
                        let schemaField = schemaFields[fieldName];
                        if (!schemaField) {
                            throw new HTTPError(500, `GraphQL type ${typeName} has no field ${fieldName}`);
                        }
                        let fnConfig = fields[fieldName];
                        schemaField.resolve = CompileResolver(typeName, fieldName, fnConfig, this);
                    }
                } else if (schemaType instanceof gq.GraphQLInterfaceType) {
                    for (let fieldName of Object.getOwnPropertyNames(fields)) {
                        if (fieldName == "__typename") {
                            schemaType.resolveType = CompileTypeNameResolver(typeName,fields[fieldName], this);
                        } else {
                            throw new HTTPError(500, `GraphQL interface type ${typeName} may only have a '__typename' resolver`);
                        }
                    }
                } else {
                    throw new HTTPError(500, `GraphQL type ${typeName} is a not an object or interface, and cannot have resolvers`);
                }
            }
        }
    }


    makeContext(credentials: Credentials | null, mutationAllowed: boolean) {
        if (credentials || !mutationAllowed) {
            let user = new UserImpl(this, credentials);
            let ctx = new ContextImpl(user, this.adminContext.user, mutationAllowed);
            user.context = ctx;
            return ctx;
        } else {
            return this.adminContext;
        }
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
        return this.getFunction(name)(context, args);
    }


    query(context: Context,
          fnName: string,
          n1ql: string,
          args: Args | undefined) : JSONObject[] {
        return this.upstream.query(fnName, n1ql, args, context.user);
    }


    async graphql(context: Context,
                  query: string,
                  variableValues?: Args,
                  operationName?: string) : Promise<gq.ExecutionResult> {
        console.debug(`GRAPHQL ${query}`);
        if (!this.schema) throw new HTTPError(404, "No GraphQL schema");
        return gq.graphql({
            contextValue: context,
            schema: this.schema,
            source: query,
            variableValues: variableValues,
            operationName: operationName,
        });
    }


    get(docID: string, user: User) : Document | null {
        return this.upstream.get(docID, user);
    }

    save(doc: Document, docID: string | undefined, user: User) : string | null {
        return this.upstream.save(doc, docID, user);
    }

    delete(docID: string, revID: string | undefined, user: User) : boolean {
        return this.upstream.delete(docID, revID, user);
    }


    readonly schema?: gq.GraphQLSchema;     // Compiled GraphQL schema (with resolvers)

    private adminContext: ContextImpl;          // The admin Context (only one is needed)
    private functions: Record<string,JSFn>; // Compiled JS functions
}


//////// CONTEXT IMPLEMENTATION


class ContextImpl implements Context {
    constructor(readonly user: User,
                readonly admin: User,
                mutationAllowed: boolean) {
        if (!mutationAllowed) {
            console.debug("++++ Context starts read-only");
            this.readOnlyLevel++;
        }
    }

    checkUser(name: string | string[]) : boolean {
        return this.user.isAdmin || match(name, this.user.name!);
    }

    requireUser(name: string | string[]) {
        if (!this.checkUser(name)) throw new HTTPError(403, "Permission denied (user)");
    }

    checkAdmin() : boolean {
        return this.user.isAdmin;
    }

    requireAdmin() {
        if (!this.checkAdmin()) throw new HTTPError(403, "Permission denied (admin only)");
    }

    checkRole(role: string | string[]) : boolean {
        if (this.user.isAdmin) return true;
        for (let myRole of this.user.roles!) {
            if (match(role, myRole))  return true;
        }
        return false;
    }

    requireRole(role: string | string[]) {
        if (!this.checkRole(role)) throw new HTTPError(403, "Permission denied (role)");
    }

    checkAccess(channel: string | string[]) : boolean {
        if (this.user.isAdmin) return true;
        for (let myChannel of this.user.channels!) {
            if (match(channel, myChannel))  return true;
        }
        return false;
    }

    requireAccess(channel: string | string[]) {
        if (!this.checkAccess(channel)) throw new HTTPError(403, "Permission denied (channel)");
    }

    checkAllowed(allow: AllowConfig | undefined) : boolean {
        return this.user.isAdmin
            || (allow !== undefined && (
                    (allow.users !== undefined    && allow.users.includes(this.user.name!)) ||
                    (allow.roles !== undefined    && this.checkRole(allow.roles)) ||
                    (allow.channels !== undefined && this.checkAccess(allow.channels))));
    }

    requireAllowed(allow: AllowConfig | undefined) {
        if (!this.checkAllowed(allow)) throw new HTTPError(403, "Permission denied");
    }

    checkMutating() : boolean {
        return this.readOnlyLevel == 0;
    }

    requireMutating() : void {
        if (!this.checkMutating()) throw new HTTPError(403, "Permission denied (mutating)");
    }

    readOnlyLevel = 0;
}

export function BeginReadOnly(context: Context) {
    if (!context.user.isAdmin) {
        console.debug("++++ BeginReadOnly");
        (context as ContextImpl).readOnlyLevel++;
    }
}

export function EndReadOnly(context: Context) {
    if (!context.user.isAdmin) {
        console.debug("---- EndReadOnly");
        (context as ContextImpl).readOnlyLevel--;
    }
}


//////// CRUD IMPLEMENTATION


class CRUDImpl implements CRUD {

    constructor(db: DatabaseImpl, collectionName: string, user: UserImpl) {
        this.db = db;
        // this.collection = collectionName;
        this.user = user;
    }


    get(docID: string) : Document | null {
        return this.db.get(docID, this.user);
    }


    save(doc: Document, docID?: string) : string | null {
        if (!this.user.canMutate)
            throw new HTTPError(403, "save() is not allowed in a read-only context");
        return this.db.save(doc, docID, this.user);
    }


    delete(docOrID: string | Document) : boolean {
        if (!this.user.canMutate)
            throw new HTTPError(403, "delete() is not allowed in a read-only context");
        if (typeof docOrID === 'string') {
            return this.db.delete(docOrID, undefined, this.user);
        } else {
            let id = docOrID['_id'];
            if (!id) throw "delete() called with doc object that has no '_id' property";
            return this.db.delete(id, docOrID._rev, this.user);
        }
    }

    private db: DatabaseImpl;
    // private collection: string;  // TODO: support collections
    private user: UserImpl;         // The User I access it as
}


//////// USER IMPLEMENTATION


export let CallDepth = 1;
export const MaxCallDepth = 20;


class UserImpl implements User {

    constructor(private db: DatabaseImpl,
                credentials: Credentials | null)
    {
        if (credentials) {
            [this.name, this.roles, this.channels] = credentials;
        }
        this.defaultCollection = new CRUDImpl(db, '_default', this);
    }


    // Authorization:

    readonly name?: string;
    readonly roles?: string[];
    readonly channels?: string[];

    get isAdmin() {return this.name === undefined;}

    get canMutate() : boolean {
        return this.isAdmin || this.context.checkMutating();
    }


    // API:

    readonly defaultCollection: CRUD;


    function(name: string, args?: Args) : unknown {
        let fn = this.db.getFunction(name);
        if (++CallDepth > MaxCallDepth) {
            console.error("User function recursion too deep");
            throw new HTTPError(508, "User function recursion too deep");
        }
        try {
            return fn(this.context, args);
        } finally {
            --CallDepth;
        }
    }


    async graphql(query: string, args?: Args) : Promise<JSONObject | null> {
        if (++CallDepth > MaxCallDepth) {
            console.error("User function recursion too deep");
            throw new HTTPError(508, "User function recursion too deep");
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


// Returns true if `what` is equal to `against` or included in it.
function match(what: string | string[], against: string) {
    if (typeof(what) === 'string') {
       return (what == against);
    } else {
       return what.includes(against);
   }
}
