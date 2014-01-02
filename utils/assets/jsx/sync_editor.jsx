/** @jsx React.DOM */

window.SyncPage = React.createClass({
  render : function() {
    /*jshint ignore:start */
    return (
      <div className="SyncPage">
        <p>The <strong>Sync Function</strong> determines application-specific behavior regarding who can see and modify which documents. The code you write here can validate updates, route documents to channels, and grant access privileges  to users and groups on a per-channel basis. For more information <a href="http://docs.couchbase.com/sync-gateway/#sync-function-api">see the Sync Function API documentation.</a></p>
        <p>Try some examples:
        <ul className="defaults">
        <li><a onClick={""/*this.handleExampleClick.bind(this, "basic")*/}>basic</a> - {"the default Sync Function used when the application doesn't specify one"}</li>
        <li>personal data - just sync my data to my devices, no sharing</li>
        <li>social rooms - todos, chat, photo sharing, can all use a room membership model.</li>
        </ul>
        </p>
        <SyncFunEditor db={this.props.db}/>
      </div>
      )
    /*jshint ignore:end */
  }
})

var examples = {
  "basic" : "function(doc){\n  channel(doc.channels)\n}"
}


var SyncFunEditor = React.createClass({
  render : function() {
    console.log("SyncFunEditor", this.state, this.props)
    return <div className="SyncFunEditor">
      <SyncFunctionForm db={this.props.db}/>
      <SyncPreview db={this.props.db}/>
    </div>
  }
})

window.DocSyncPreview = React.createClass({
  getDefaultProps : function(){
    return {sync:{channels:[], access:{}}};
  },
  render : function() {
    var sync = this.props.sync;
    // console.log("sync", sync)
    var db = this.props.db;
    if (!sync) return <div></div>;
    var channels = sync.channels;
    return <div className="DocSyncPreview">
      <div className="channels">
        <h4>Channels</h4>
        <ul>
        {channels.map(function(ch) {
          return <li>{channelLink(db, ch)}</li>
        })}
        </ul>
      </div>
      <AccessList access={sync.access} db={db}/>
    </div>;
    }
})

var SyncFunctionForm = React.createClass({
  getInitialState : function() {
    return {}
  },
  bindState: function(name) {
    return function(value) {
      var newState = {};
      newState[name] = value;
      this.setState(newState);
    }.bind(this);
  },
  componentDidMount : function(elem){
    dbState(this.props.db).on("connected", function(){
      var sync = dbState(this.props.db).getSyncFunction()
      this.setState({code : sync})
    }.bind(this))
  },
  previewClicked : function(e) {
    e.preventDefault();
    var dbs = dbState(this.props.db)
    dbs.on("connected", function() {
      console.log("setSyncFunction")
      dbs.setSyncFunction(this.state.code)
    }.bind(this))
  },
  deployClicked : function(e) {
    e.preventDefault();
    var yes = confirm("Are you sure? If you are using in-memory Walrus this will erase your data.")
    if (yes) {
      dbState(this.props.db).deploySyncFunction(this.state.code, function(err){
        // if (err) throw(err);
        console.log("deployed")
        // window.location.reload();
      })
    }
  },
  render : function() {
    var editor = this.state.code ? <CodeMirrorEditor
        onChange={this.bindState('code')}
        className="SyncFunctionCodeEditor"
        codeText={this.state.code} /> : <div/>
    return <form className="SyncFunctionCode">
      <h3>Sync Function</h3>
      {editor}
      <div className="SyncFunctionCodeButtons">
        <button onClick={this.previewClicked}>Live Preview Mode</button>
        <button onClick={this.deployClicked}>Deploy To Server</button>
      </div>
    </form>
  }
})

var SyncPreview = React.createClass({
  getInitialState : function() {
    return {}
  },
  setDoc : function(id) {
    if (!id) return;
    dbState(this.props.db).getDoc(id, function(err, doc, deployedSync, previewSync) {
      if (err) { return console.error(err)}
      this.setState({docID : id, doc : doc, deployed : deployedSync, preview : previewSync})
    }.bind(this))
  },
  handleRandomAccessDoc : function() {
    this.setDoc(dbState(this.props.db).randomAccessDocID())
  },
  handleRandomDoc : function() {
    this.setDoc(dbState(this.props.db).randomDocID())
  },
  componentDidMount : function(){
    var dbS = dbState(this.props.db);
    dbS.on("connected", function(){
      this.setDoc(dbS.randomAccessDocID() || dbS.randomDocID())
    }.bind(this))
  },
  render : function() {
    console.log("SyncPreview", this.state, this.props)
    return <div className="SyncPreview">
      <JSONDoc db={this.props.db} doc={this.state.doc} id={this.state.docID}/>
      <div className="docs">
        <p>Preview sync function on real documents:
        <ul className="defaults">
          <li><a onClick={this.handleRandomDoc}>random</a></li>
          <li><a onClick={this.handleRandomAccessDoc}>grants access</a></li>
        </ul>
        </p>
      </div>
      <br className="clear"/>
      <p>Preview results:</p>
      <DocSyncPreview db={this.props.db} id={this.state.docID} sync={this.state.preview}/>
      <br className="clear"/>
      <p>Deployed results:</p>
      <DocSyncPreview db={this.props.db} id={this.state.docID} sync={this.state.deployed}/>
      </div>
  }
})




var IS_MOBILE = (
  navigator.userAgent.match(/Android/i)
    || navigator.userAgent.match(/webOS/i)
    || navigator.userAgent.match(/iPhone/i)
    || navigator.userAgent.match(/iPad/i)
    || navigator.userAgent.match(/iPod/i)
    || navigator.userAgent.match(/BlackBerry/i)
    || navigator.userAgent.match(/Windows Phone/i)
);

var CodeMirrorEditor = React.createClass({
  componentDidMount: function(root) {
    if (IS_MOBILE) {
      return;
    }
    this.editor = CodeMirror.fromTextArea(this.refs.editor.getDOMNode(), {
      mode: 'javascript',
      lineNumbers: true,
      matchBrackets: true,
      readOnly: this.props.readOnly
    });
    // console.log("CodeMirror",this.editor)
    this.editor.on('change', this.onChange);
  },
  onChange: function() {
    if (this.props.onChange) {
      var content = this.editor.getValue();
      this.props.onChange(content);
    }
  },
  render: function() {
    // wrap in a div to fully contain CodeMirror
    var editor;
    // console.log("editor", this.props)
    if (IS_MOBILE) {
      editor = <pre style={{overflow: 'scroll'}}>{this.props.codeText}</pre>;
    } else {
      editor = <textarea ref="editor" defaultValue={this.props.codeText} />;
    }

    return (
      <div className={this.props.className}>
        {editor}
      </div>
    );
  }
});
