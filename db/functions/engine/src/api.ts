import { Args, User, Config, Database, Context, Credentials, Document, JSONObject } from './types'
import { MakeDatabase, Upstream } from "./impl";

declare let sg_console: {
    debug: any;
    log: any;
    error: any;
}

console.debug = sg_console.debug;
console.log = sg_console.log;
console.error = sg_console.error;


/** The interface the native code needs to implement. */
export interface NativeAPI {
    query(fnName: string,
          n1ql: string,
          argsJSON: string | undefined,
          asAdmin: boolean) : string;
    get(docID: string,
        asAdmin: boolean) : string | null;
    save(docJSON: string,
         docID: string | undefined,
         asAdmin: boolean) : string | null;
    delete(docID: string,
           revID: string | undefined,
           asAdmin: boolean) : boolean;
}


// Wraps a NativeAPI and exposes it as an Upstream for a Database to use
class UpstreamNativeImpl implements Upstream {
    constructor(private native: NativeAPI) { }

    query(fnName: string, n1ql: string, args: Args | undefined, user: User) : JSONObject[] {
        let result = this.native.query(fnName, n1ql, this.stringify(args), user.isAdmin);
        return JSON.parse(result);
    }

    get(docID: string, user: User) : Document | null {
        let jresult = this.native.get(docID, user.isAdmin);
        if (jresult === null) return jresult;
        return this.parseDoc(jresult)
    }

    save(doc: object, docID: string | undefined, user: User) : string | null {
        return this.native.save(JSON.stringify(doc), docID, user.isAdmin);
    }

    delete(docID: string, revID: string | undefined, user: User) : boolean {
        return this.native.delete(docID, revID, user.isAdmin);
    }

    private stringify(obj: object | undefined) : string | undefined {
        return obj ? JSON.stringify(obj) : undefined;
    }

    private parseDoc(json: string) : Document {
        let result = JSON.parse(json)
        if (typeof(result) !== "object")
            throw Error("NativeAPI returned JSON that's not an Object");
        return result as Document
    }
}


/** The API this module implements. */
export class API {
    constructor(configJSON: string, native: NativeAPI) {
        let config = JSON.parse(configJSON) as Config;
        this.db = MakeDatabase(config.functions, config.graphql, new UpstreamNativeImpl(native));
    }

    /** Calls a named function. */
    callFunction(name: string,
                 argsJSON: string | undefined,
                 user: string | undefined,
                 roles?: string,
                 channels?: string) : string | Promise<string>
    {
        let args = argsJSON ? JSON.parse(argsJSON) : undefined;
        let result = this.db.callFunction(name, args, this.makeCredentials(user, roles, channels));
        if (result instanceof Promise) {
            return result.then( result => JSON.stringify(result) );
        } else {
            return JSON.stringify(result);
        }
    }

    /** Runs a GraphQL query. */
    graphql(query: string,
            variablesJSON: string | undefined,
            user: string | undefined,
            roles?: string,
            channels?: string) : Promise<string>
    {
        let vars = variablesJSON ? JSON.parse(variablesJSON) : undefined;
        return this.db.graphql(query, vars, this.makeContext(user, roles, channels))
            .then( result => JSON.stringify(result) );
    }

    private makeCredentials(user?: string, roles?: string, channels?: string) : Credentials | null {
        if (user === undefined) return null;
        return [user,
                roles?.split(',') ?? [],
                channels?.split(',') ?? []];
    }

    private makeContext(user?: string, roles?: string, channels?: string) : Context {
        let cred = this.makeCredentials(user, roles, channels)
        return this.db.makeContext(cred);
    }

    private db: Database;
};


export function main(configJSON: string, native: NativeAPI) : API {
    return new API(configJSON, native);
}
