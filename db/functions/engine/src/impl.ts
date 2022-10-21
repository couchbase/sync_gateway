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
        this.adminContext = new ContextImpl(adminUser, adminUser);
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


    makeContext(credentials: Credentials | null) {
        if (credentials) {
            let user = new UserImpl(this, credentials);
            let ctx = new ContextImpl(user, this.adminContext.user);
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


    callFunction(name: string,
                 args: Args | undefined,
                 credentials: Credentials | null)
    {
        let ctx = this.makeContext(credentials);
        return this.getFunction(name)(ctx, args);
    }


    query(fnName: string,
                n1ql: string,
                args: Args | undefined,
                context: Context) : JSONObject[] {
        return this.upstream.query(fnName, n1ql, args, context.user);
    }


    async graphql(query: string, args: Args | undefined, context: Context) : Promise<gq.ExecutionResult> {
        console.debug(`GRAPHQL ${query}`);
        if (!this.schema) throw new HTTPError(404, "No GraphQL schema");
        return gq.graphql({
            schema: this.schema,
            source: query,
            variableValues: args,
            contextValue: context,
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
        readonly admin: User) { }

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
}


//////// CRUD IMPLEMENTATION


class CRUDImpl implements CRUD {

    constructor(db: DatabaseImpl, collectionName: string, user: User) {
        this.db = db;
        // this.collection = collectionName;
        this.user = user;
    }


    get(docID: string) : Document | null {
        return this.db.get(docID, this.user);
    }


    save(doc: Document, docID?: string) : string | null {
        return this.db.save(doc, docID, this.user);
    }


    delete(docOrID: string | Document) : boolean {
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
    private user: User;                        // The User I access it as
}


//////// USER IMPLEMENTATION


export let CallDepth = 1;
export const MaxCallDepth = 20;


class UserImpl implements User {

    constructor(db: DatabaseImpl, credentials: Credentials | null) {
        this.db = db;
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


    // API:

    readonly defaultCollection: CRUD;


    function(name: string, args?: Args) : unknown {
        let fn = this.db.getFunction(name);
        if (++CallDepth > MaxCallDepth) throw new HTTPError(500, "User function recursion too deep");
        try {
            return fn(this.context, args);
        } finally {
            --CallDepth;
        }
    }


    async graphql(query: string, args?: Args) : Promise<JSONObject | null> {
        if (++CallDepth > MaxCallDepth) throw new HTTPError(500, "User function recursion too deep");
        try {
            let result = await this.db.graphql(query, args, this.context);
            if (result.errors) throw "GraphQL error"; //TODO: Expose the errors
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

    private db: DatabaseImpl;
    context!: Context;
};


// Returns true if `what` is equal to `against` or included in it.
function match(what: string | string[], against: string) {
    if (typeof(what) === 'string') {
       return (what == against);
    } else {
       return what.includes(against);
   }
}
