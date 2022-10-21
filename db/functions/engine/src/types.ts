import * as gq from 'graphql';


//////// CONFIGURATION

type MaybePromise<T> = T | Promise<T>

export type JSONObject = { [key: string]: undefined }

/** Named arguments to a function call. */
export type Args = { [key:string]: any};

/** JavaScript function. May return a Promise. */
export type JSFn = (context: Context, args?: Args) => unknown;

/** JavaScript GraphQL resolver function. */
export type ResolverFn = (source: any,
                          args: Args,
                          context: Context,
                          info: ResolveInfo) => undefined;

export interface ResolveInfo extends gq.GraphQLResolveInfo {
    readonly selectedFieldNames : string[];
};

/** Authorization for a function. */
export type AllowConfig = {
    users?:    string[],    // Names of allowed users
    roles?:    string[],    // Allowed roles
    channels?: string[],    // Allowed channels
};

/** Defines a function or GraphQL resolver. */
export type FunctionConfig = {
    type:   "query" | "javascript",         // Language the 'code' is in
    code:   string,                         // The function's JavaScript code or N1QL query
    args?:  string[],                       // Names of parameters (not used by GraphQL)
    allow?: AllowConfig,                    // Who's allowed to call this
};

/** Functions configuration: maps function name to its config. */
export type FunctionsConfig = {
    definitions: Record<string,FunctionConfig>
};

export type FieldMap = Record<string,FunctionConfig>;
export type ResolverMap = Record<string,FieldMap>;

/** GraphQL configuration. */
export type GraphQLConfig = {
    schema?:     string,        // The schema itself
    schemaFile?: string,        // Path to schema file (only if schema is not given)
    resolvers:   ResolverMap,   // GraphQL resolver functions
};

export type Config = {
    functions?: FunctionsConfig;
    graphql?:   GraphQLConfig;
}


//////// RUNTIME CONTEXT


/** Context object passed to all functions. */
export interface Context {
    readonly user: User;
    readonly admin: User;

    checkUser(name: string | string[]) : boolean;
    requireUser(name: string | string[]) : void;
    checkAdmin() : boolean;
    requireAdmin() : void;
    checkRole(role: string | string[]) : boolean;
    requireRole(role: string | string[]) : void;
    checkAccess(channel: string | string[]) : boolean;
    requireAccess(channel: string | string[]) : void;
}


/** The type of the `context.user` and `context.admin` objects.
 *  Exposes auth and APIs scoped to either the current user, or to an admin. */
 export interface User {
    readonly name?: string;
    readonly roles?: string[];
    readonly channels?: string[];

    readonly isAdmin : boolean;

    readonly defaultCollection: CRUD;

    function(name: string, args?: Args) : unknown;
    graphql(query: string, args?: Args) : Promise<JSONObject | null | undefined>;
};


export interface Document {
    _id? : string;
    _rev? : string;
};


/** The type of the `User.defaultCollection` object. Exposes database CRUD APIs. */
export interface CRUD {
    get(docID: string) : Document | null;
    save(doc: Document, docID?: string) : string | null;
    delete(docID: string) : boolean;
    delete(doc: Document) : boolean;
}


//////// UTILITIES


/** An exception that conveys an HTTP status. */
export class HTTPError extends Error {
    constructor(readonly status: number, message: string) {
        super(message);
        this.status = status;
    }

    override toString() {
        return `[${this.status}] ${super.toString()}`;
    }
}


//////// DATABASE


/** User credentials: tuple of [username, roles, channels] */
export type Credentials = [string, string[], string[]];


/** Top-level object that stores the compiled state for a database. */
export interface Database {
    /** Creates an execution context given a user's name, roles and channels. */
    makeContext(credentials: Credentials | null) : Context;

    /** Calls a named function. */
    callFunction(name: string, args: Args | undefined, credentials: Credentials | null) : MaybePromise<unknown>;

    /** Runs a N1QL query. Called by functions of type "query". */
    query(fnName: string, n1ql: string, args: Args | undefined, context: Context) : JSONObject[];

    /** Runs a GraphQL query. */
    graphql(query: string, args: Args | undefined, context: Context) : Promise<gq.ExecutionResult>;

    /** The compiled GraphQL schema. */
    readonly schema?: gq.GraphQLSchema;
}
