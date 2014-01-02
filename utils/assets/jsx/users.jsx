/** @jsx React.DOM */


var sg = require("coax")(location.origin);

window.UsersPage = React.createClass({
  render : function() {
    var db = this.props.db;
    var userID = this.props.userID;
    console.log("render UsersPage")
    return (
      /*jshint ignore:start */
      <div>
      <UsersForDatabase db={db}/>
      <UserInfo db={db} userID={userID}/>
      </div>
      /*jshint ignore:end */
    );
  }
});

var UserInfo = React.createClass({
  mixins : [StateForPropsMixin],
  getInitialState: function() {
    return {db:this.props.db};
  },
  setStateForProps : function(props) {
    if (props.db && props.userID)
      sg.get([props.db, "_user", props.userID], function(err, data) {
        this.setState({user : data, userID : props.userID, db : props.db})
      }.bind(this))
    else
      this.setState(this.getInitialState())
  },
  render : function() {
    var user = this.state.user, userID = this.state.userID, db = this.state.db;
    if (!this.props.userID) return <div></div>;
    if (!user) return <div>No such user: {this.props.userID} Perhaps you are browsing a partial replica?</div>;
    return (
      /*jshint ignore:start */
      <div className="UserInfo">
      <h2>{user.name}</h2>
      <div className="UserChannels">
            <h3>Channels <a href={dbLink(db,"channels?title=Channels for "+user.name+"&watch="+user.all_channels.join(','))}>(watch all)</a></h3>
            <ul>
      {
        user.all_channels.map(function(ch){
          return <li>{channelLink(db, ch)}</li>
        })
      }
            </ul>
      </div>
      <div className="UserDoc">
        <h3>JSON User Document</h3>
        <pre><code>{JSON.stringify(user, null, 2)}</code></pre>
        // <a href={dbLink(db, "_user/"+userID+"/edit")}>edit</a>
      </div>
      </div>
      /*jshint ignore:end */
    );
  }
});

window.UsersForDatabase = React.createClass({
  getInitialState: function() {
    return {users: []};
  },
  componentWillMount: function() {
    var w = this;
    console.log("load ListUsers")
    sg.get([this.props.db, "_view", "principals"], function(err, data) {
      console.log("got", data)
      w.setState({users : data.rows.filter(function(r) {
        return r.key;
      })})
    });
  },
  render : function() {
    var db = this.props.db;
    var users = this.state.users;
    console.log(users, "users")
    /*jshint ignore:start */
    return (<div className="UsersForDatabase">
      <p>{users.length} user{users.length !== 1 && "s"} in {db}</p>
      <ul >
            {users.map(function(user) {
              return <li key={user.key}><a href={"/_utils/db/"+db+"/users/"+user.key}>{user.key}</a></li>;
            })}
          </ul></div>)
  }
})
