/** @jsx React.DOM */

function hrefToggleWatchingChannel(db, chName, current) {
  var channels = [];
  var urlparts = current.split("?");
  var query = urlparts[1]
  if (query) {
    var parts = query.split(/=|&/)
    var watch = parts.indexOf("watch")
    if (watch !== -1) {
      channels = parts[watch+1].split(',');
    }
  }
  var chIndex = channels.indexOf(chName);
  if (chIndex == -1) {
    channels.push(chName)
  } else {
    channels.splice(chIndex, 1)
  }
  if (channels.length == 0) {
    return urlparts[0];
  } else {
    return dbLink(db, "channels?watch="+channels.join(','))
  }
}

window.ChannelsWatchPage = React.createClass({
  render : function(){
    var channels = this.props.watch;
    var db = this.props.db,
      title = this.props.title || "Watch Channels";
// todo use cookies to offer previous watch list if the cookie differs from url param
    return (
      <div className="ChannelGrid">
      <h2>{title}</h2>
      <RecentChannels db={db} watch={this.props.watch}/>
      <ul>
      {channels.map(function(ch){
        return <li key={"ChannelChanges"+ch}><ChannelChanges id={ch} db={db}/></li>
      })}
      </ul>
      </div>
    )
  }
})

window.ChannelInfoPage = React.createClass({
  mixins : [StateForPropsMixin, EventListenerMixin],
  getInitialState: function() {
    return {channel: {}};
  },
  channelChanged : function() {
    this.setState({
      channel : dbState(this.props.db).channel(this.props.id)
    })
  },
  setStateForProps : function(newProps, oldProps) {
    if (newProps.db && newProps.id) {
      var dbs = dbState(newProps.db)
      this.listen(dbs, "ch:"+newProps.id, this.channelChanged)
      this.channelChanged()
    }
  },
  render : function() {
    var channel = this.state.channel || {};
    /*jshint ignore:start */
    return (
      <div>
        <h2>Channel: {this.props.id}</h2>
        <div className="channelDocs">
          <h3>Recent Updates</h3>
          <ChannelChanges id={this.props.id} db={this.props.db}/>
        </div>
        <ChannelAccessList access={channel.access} db={this.props.db}/>
      </div>
      )
    /*jshint ignore:end */
  }
})

// smells like AccessList
var ChannelAccessList = React.createClass({
  render : function() {
    var db = this.props.db;
    var accessList = []
    for (var docid in this.props.access) {
      accessList.push({id: docid, users: this.props.access[docid]})
    }
    return <div className="ChannelAccessList">
    <h4>Access</h4>
    <dl>
    {accessList.map(function(ch) {
      return <span><dt key={"dt"+ch.id}>{docLink(db, ch.id)}</dt>
        {ch.users.map(function(who){
            return <dd key={"dd"+ch.id+who}>{userLink(db, who)}</dd>
          })}</span>
    })}
    </dl>
  </div>
  }
})

var ChannelChanges = React.createClass({
  mixins : [StateForPropsMixin, EventListenerMixin],
  getInitialState: function() {
    return {channel: {changes:[]}, db : this.props.db, id : this.props.id};
  },
  channelChanged : function() {
    // console.log("channelChanged bug")
    this.setState({
      channel : dbState(this.props.db).channel(this.props.id)
    })
  },
  setStateForProps : function(newProps, oldProps) {
    var dbs = dbState(newProps.db)
    this.listen(dbs, "ch:"+newProps.id, this.channelChanged)
    this.listen(dbs, "syncReset", this.channelChanged)
    this.channelChanged()
  },
  render : function() {
    // console.log("render channelChanges", this.props.id)
    var channel = this.state.channel;
    var db = this.state.db, hiddenAccess = <span></span>;
    if (channel.hiddenAccessIds && channel.hiddenAccessIds.length > 0) {
      hiddenAccess = <span><ul>{
                channel.hiddenAccessIds.map(function(id){
                  return <li className="isAccess">Hidden: <a href={dbLink(db, "documents/"+id)}>{id}</a></li>
                })
              }</ul></span>
    }
    return (
      <div className="ChannelChanges">
      <a className="watched" href={dbLink(db, "channels/"+channel.name)}>{channel.name}</a>
      {hiddenAccess}
    <ul>
      {channel.changes.map(function(ch){
        return <li key={channel.name+ch.id} className={ch.isAccess && "isAccess"}>{ch.seq} : <a href={dbLink(db, "documents/"+ch.id)}>{ch.id}</a></li>
      })}
    </ul></div>
    );
  }
})

window.RecentChannels = React.createClass({
  mixins : [StateForPropsMixin, EventListenerMixin],
  getInitialState: function() {
    return {channelNames: [], db : this.props.db};
  },
  changed : function() {
    var oldNames = this.state.channelNames,
      newNames = dbState(this.props.db).channelNames()
      console.log("RecentChannels state", newNames)
    if (oldNames.sort().join() !== newNames.sort().join()) {
      this.setState({
        channelNames : newNames
      })
    }
  },
  setStateForProps : function(newProps, oldProps) {
    var dbs = dbState(newProps.db)
    this.listen(dbs, "change", this.changed)
    this.changed()
  },
  render : function() {
    // console.log("render RecentChannels", this.state, this.props)
    var watch = this.props.watch || [],
      currentLoc = location.toString(),
      channelNames = this.state.channelNames,
      db = this.state.db;
    /*jshint ignore:start */
    return (<div className="RecentChannels">
      <strong>{channelNames.length} channels</strong>.
      Select channels to watch updates. <a href={"/"+db+"/_changes?limit=5"}>raw</a>
      <ul>
      {this.state.channelNames.map(function(ch) {
        var isWatched = watch.indexOf(ch) !== -1
        return <li key={ch+isWatched}>
          <a className={isWatched && "watched"} href={hrefToggleWatchingChannel(db, ch, currentLoc)}>
            {ch}
          </a>
          </li>;
      })}
    </ul></div>)
    /*jshint ignore:end */
  }
});
