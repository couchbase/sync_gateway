/**
 * @jsx React.DOM
 */

window.AllDatabases = React.createClass({
  loadList : function() {
    sg.get("_all_dbs", function(err, list) {
      console.log("got alldbs", this.state)
      this.setState({dbs: list});
    }.bind(this));
  },
  getInitialState: function() {
    return {dbs: []};
  },
  componentWillMount: function() {
    console.log("alldbs")
    this.loadList();
  },
  createDatabase : function(e) {
    e.preventDefault();
    var db = this.refs.dbName.state.value,
      url =this.refs.dbServer.state.value
    // console.log("createDatabase", db, url)
    sg.put(db, {
      server : url
    }, function(err){
      var message;
      if (err && err.constructor !== SyntaxError) {
        message = err.message
      } else {
        message = "Created database: "+db
      }
      this.refs.dbName.state.value = ""
      this.loadList();
      console.log("setState", message)
      this.setState({message : message})
    }.bind(this))
  },
  render : function() {
    console.log("alldbs",this.state)
    var dbs = this.state.dbs;
    var title = this.props.title || "Databases"
    return (<div>
      <h3>{title}</h3>
      <ul className="defaults">
      {dbs.map(function(name) {
        return <li key={name}><a href={dbLink(name)}>{name}</a></li>;
      })}
      </ul>
      <form>
        <p>{this.state.message}</p>
        <label>Database Name: <input size="60" placeholder="lowercase" ref="dbName"/></label>
        <br/>
        <label>Storage URL: <input size="60" defaultValue="walrus:" ref="dbServer"/></label>
        <button onClick={this.createDatabase}>Create new database.</button>
      </form>
    </div>);
  }
});
