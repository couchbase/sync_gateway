import * as gq from 'graphql';


//////// CONFIGURATION


/** Named arguments to a function call. */
export type Args = {[key:string]:any};

/** JavaScript function. */
export type JSFn = (context: Context, args?: Args) => Promise<any>;

/** JavaScript GraphQL resolver function. */
export type ResolverFn = (source: any,
                          args: Args,
                          context: Context,
                          info: ResolveInfo) => any;

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
export type FunctionsConfig = Record<string,FunctionConfig>;

export type FieldMap = Record<string,FunctionConfig>;
export type ResolverMap = Record<string,FieldMap>;

/** GraphQL configuration. */
export type GraphQLConfig = {
    schema?:     string,        // The schema itself
    schemaFile?: string,        // Path to schema file (only if schema is not given)
    resolvers:   ResolverMap,   // GraphQL resolver functions
    graphiql?:   boolean;       // If true, enables "GraphiQL" browser GUI
};

export type Config = {
    functions?: FunctionsConfig;
    graphql?:   GraphQLConfig;
}


//////// RUNTIME CONTEXT


/** Context object passed to all functions. */
export class Context {
    constructor(readonly user: User,
                readonly admin: User) { }
}


/** The type of the `context.user` and `context.admin` objects.
 *  Exposes auth and APIs scoped to either the current user, or to an admin. */
 export interface User {
    readonly name?: string;
    readonly roles?: string[];
    readonly channels?: string[];

    readonly isAdmin : boolean;

    checkUser(name: string | string[]) : boolean;
    requireUser(name: string | string[]) : void;
    checkRole(role: string | string[]) : boolean;
    requireRole(role: string | string[]) : void;
    checkAccess(channel: string | string[]) : boolean;
    requireAccess(channel: string | string[]) : void;

    readonly defaultCollection: CRUD;

    func(name: string, args?: Args) : Promise<any>;
    graphql(query: string, args?: Args) : Promise<any>;
};


export interface Document {
    _id? : string;
    _rev? : string;
};


/** The type of the `User.defaultCollection` object. Exposes database CRUD APIs. */
export interface CRUD {
    get(docID: string) : Promise<Document | null>;
    save(doc: Document, docID?: string) : Promise<string>;
    delete(docID: string) : Promise<boolean>;
    delete(doc: Document) : Promise<boolean>;
}


//////// UTILITIES


/** An exception that conveys an HTTP status. */
export class HTTPError extends Error {
    constructor(readonly status: number, message: string) {
        super(message);
        this.status = status;
    }

    override toString() {
        return `${this.status} ${super.toString()}`;
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
    callFunction(name: string, args: Args | undefined, credentials: Credentials | null) : Promise<any>;

    /** Runs a N1QL query. Called by functions of type "query". */
    query(fnName: string, n1ql: string, args: Args | undefined, context: Context) : Promise<any[]>;

    /** Runs a GraphQL query. */
    graphql(query: string, args: Args | undefined, context: Context) : Promise<gq.ExecutionResult>;

    /** The compiled GraphQL schema. */
    readonly schema?: gq.GraphQLSchema;
}
