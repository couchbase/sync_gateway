import { Args, User, Config, Database, Context, Credentials } from './types'
import { MakeDatabase, Upstream } from "./impl";


/** The interface the native code needs to implement. */
export interface NativeAPI {
    query(fnName: string,
          n1ql: string,
          argsJSON: string | undefined,
          asAdmin: boolean) : string;
    get(docID: string,
        asAdmin: boolean) : string | null;
    save(docJSON: string,
         docID: string,
         asAdmin: boolean) : string;
    delete(docID: string,
           revID: string | undefined,
           asAdmin: boolean) : boolean;
}


// Wraps a NativeAPI and exposes it as an Upstream for a Database to use
class UpstreamNativeImpl implements Upstream {
    constructor(private native: NativeAPI) { }

    query(fnName: string, n1ql: string, args: Args | undefined, user: User) : Promise<any[]> {
        let result = this.native.query(fnName, n1ql, this.stringify(args), user.isAdmin);
        return Promise.resolve(JSON.parse(result));
    }

    get(docID: string, user: User) : Promise<object | null> {
        let result = this.native.get(docID, user.isAdmin);
        return Promise.resolve(this.parse(result));
    }

    save(doc: object, docID: string, user: User) : Promise<string> {
        let result = this.native.save(JSON.stringify(doc), docID, user.isAdmin);
        return Promise.resolve(result);
    }

    delete(docID: string, revID: string | undefined, user: User) : Promise<boolean> {
        let result = this.native.delete(docID, revID, user.isAdmin);
        return Promise.resolve(result);
    }

    private stringify(obj: object | undefined) : string | undefined {
        return obj ? JSON.stringify(obj) : undefined;
    }
    private parse(json: string | undefined | null) : any {
        return json ? JSON.parse(json) : undefined;
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
                 roles?: string[],
                 channels?: string[]) : Promise<string>
    {
        let args = argsJSON ? JSON.parse(argsJSON) : undefined;
        return this.db.callFunction(name, args, this.makeCredentials(user, roles, channels))
            .then( result => JSON.stringify(result) );
    }

    /** Runs a GraphQL query. */
    graphql(query: string,
            variablesJSON: string | undefined,
            user: string | undefined,
            roles?: string[],
            channels?: string[]) : Promise<string>
    {
        let vars = variablesJSON ? JSON.parse(variablesJSON) : undefined;
        return this.db.graphql(query, vars, this.makeContext(user, roles, channels))
            .then( result => JSON.stringify(result) );
    }

    private makeCredentials(user?: string, roles?: string[], channels?: string[]) : Credentials | null {
        if (user !== undefined) {
            return [user, roles ?? [], channels ?? []];
        } else {
            return null;
        }
    }

    private makeContext(user?: string, roles?: string[], channels?: string[]) : Context {
        return this.db.makeContext(this.makeCredentials(user, roles, channels));
    }

    private db: Database;
};


export function main(configJSON: string, native: NativeAPI) : API {
    return new API(configJSON, native);
}
