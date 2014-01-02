/** @jsx React.DOM */


var sg = require("coax")(location.origin);

window.DocumentsPage = React.createClass({
  render : function() {
    var db = this.props.db;
    var docID = this.props.docID;
    return (
      /*jshint ignore:start */
      <div>
      <ListDocs db={db}/>
      {docID && <DocInfo db={db} docID={docID}/>}
      </div>
      /*jshint ignore:end */
    );
  }
});

var ListDocs = React.createClass({
  getInitialState: function() {
    return {rows: []};
  },
  componentWillMount: function() {
    console.log("load ListDocs")
    var dbs = dbState(this.props.db)
    dbs.on("connected", function() {
      dbs.allDocs(function(err, rows){
        this.setState({rows : rows})
      }.bind(this));
    }.bind(this))
  },
  render : function() {
    var db = this.props.db;
    var rows = this.state.rows;
    /*jshint ignore:start */
    return <div className="ListDocs">
          <strong>{rows.length} documents</strong>, highlighted documents have access control output with the current sync function.
          <ul>
          {rows.map(function(r) {
            return <li className={r.access && "isAccess"} key={"docs"+r.id}>
              <a href={dbLink(db, "documents/"+r.id)}>
                {r.id}
              </a>
              </li>;
          })}
        </ul></div>
  }
})

window.JSONDoc = React.createClass({
  render : function() {
    return <div className="JSONDoc">
      <h4>{
        this.props.id ? <span>{this.props.id+" "} <a href={"/"+this.props.db+"/_raw/"+this.props.id}>raw</a></span> : "Loading..."}</h4>
      <pre><code>
      {JSON.stringify(this.props.doc, null, 2)}
      </code></pre>
    </div>;
  }
})

// smells like ChannelAccessList
var AccessList = React.createClass({
  render : function() {
    var db = this.props.db;
    var accessList = []
    for (var ch in this.props.access) {
      accessList.push({name: ch, users: this.props.access[ch]})
    }
    return <div className="access">
    <h4>Access</h4>
    <dl>
    {accessList.map(function(ch) {
      return <span><dt>{channelLink(db, ch.name)}</dt>
        {ch.users.map(function(who){
            return <dd>{userLink(db, who)}</dd>
          })}</span>
    })}
    </dl>
  </div>
  }
})

window.DocInfo = React.createClass({
  mixins : [StateForPropsMixin],
  getInitialState: function() {
    return {doc: {}, deployed : {channels:[], access:{}}, db : this.props.db};
  },
  setDoc : function(id) {
    if (!id) return;
    dbState(this.props.db).getDoc(id, function(err, doc, deployedSync, previewSync) {
      if (err) {return console.error(err);}
      this.setState({docID : id, doc : doc, deployed : deployedSync, preview : previewSync})
    }.bind(this))
  },
  setStateForProps : function(props) {
    if (props.db && props.docID) {
      this.setDoc(props.docID)
    }
  },
  render : function() {
    console.log("render DocInfo", this.state)
    return (
      /*jshint ignore:start */
      <div className="DocInfo">
        <JSONDoc db={this.state.db} doc={this.state.doc} id={this.state.docID}/>
        <DocSyncPreview db={this.state.db} sync={this.state.deployed} id={this.state.docID}/>
        <brClear/>
        <p><a href={"/"+this.props.db+"/"+this.state.docID}>Raw document URL</a></p>
      </div>
      /*jshint ignore:end */
    );
  }
});

