(function () {
	var d = document,
	w = window,
	p = parseInt,
	dd = d.documentElement,
	db = d.body,
	dc = d.compatMode == 'CSS1Compat',
	dx = dc ? dd: db,
	ec = encodeURIComponent;
	
	
	w.CHAT = {
		msgObj:d.getElementById("message_lt"),
		screenheight:w.innerHeight ? w.innerHeight : dx.clientHeight,
		username:null,
		userid:null,
		socket:null,
		//让浏览器滚动条保持在最低部
		scrollToBottom:function(){
			w.scrollTo(0, this.msgObj.clientHeight);
		},
		//退出，本例只是一个简单的刷新
		logout:function(){
			//this.socket.disconnect();
			location.reload();
		},
		//显示消息时间
		showTime:function(d){
			d.children[0].style.display='block';
		},
		//隐藏消息时间
		hideTime:function(d){
			d.children[0].style.display='none';
		},
		//提交聊天消息内容
		submit:function(value){
			var content = d.getElementById("content").value;
			if(value=='tupian') content=value;
			if(content != ''){
				var obj = {
					userid: this.userid,
					username: this.username,
					content: content
				};
				this.socket.emit('message', obj);
				d.getElementById("content").value = '';
				var div = document.getElementById("chat");
				div.scrollTop=div.scrollHeight;
			}
			return false;
		},
		// 广播消息内容
		sendMsg:function(obj){
				var isme = (obj.userid == CHAT.userid) ? true : false;
				var dateTime=obj.content.indexOf("public/avatar")>=0 ? '<div>图片：'+obj.dateTime+'</div></div>' : '<div style="display:none">'+obj.dateTime+'</div></div>';
				var contentDiv = '<div style="cursor:pointer;" onmouseover="CHAT.showTime(this)" onmouseout="CHAT.hideTime(this)">'+obj.content+dateTime;
				var usernameDiv = '<span>'+obj.username+'</span>';

				var section = d.createElement('section');
				if(isme){
					section.className = 'user';
					section.innerHTML = contentDiv + usernameDiv;
				} else {
					section.className = 'service';
					section.innerHTML = usernameDiv + contentDiv;
				}
				CHAT.msgObj.appendChild(section);
				//CHAT.scrollToBottom();	

				var div = document.getElementById("chat");
				div.scrollTop=div.scrollHeight;
			},
		genUid:function(){
			return new Date().getTime()+""+Math.floor(Math.random()*899+100);
		},
		//更新系统消息，本例中在用户加入、退出的时候调用
		updateSysMsg:function(o, action){
			//当前在线用户列表
			var onlineUsers = o.onlineUsers;
			//当前在线人数
			var onlineCount = o.onlineCount;
			//新加入用户的信息
			var user = o.user;
			//聊天记录
			var record = o.record;
			if(CHAT.msgObj.innerText.length==0){
				for(var i=0;i<record.length;i++){
					CHAT.sendMsg(record[i]);
				}
			}
			//更新在线人数
			var userhtml = '';
			var separator = '';
			for(key in onlineUsers) {
		        if(onlineUsers.hasOwnProperty(key)){
					userhtml += separator+onlineUsers[key];
					separator = '、';
				}
		    }
			d.getElementById("onlinecount").innerHTML = '当前共有 '+onlineCount+' 人在线，在线列表：'+userhtml;
			
			//添加系统消息
			var html = '';
			var date=new Date().toLocaleTimeString();
			html += '<div class="msg-system">';
			html += user.username;
			html += (action == 'login') ? ' 加入了聊天室  ' : ' 退出了聊天室  ';
			html += date;
			html += '</div>';
			var section = d.createElement('section');
			section.className = 'system J-mjrlinkWrap J-cutMsg';
			section.innerHTML = html;
			this.msgObj.appendChild(section);	
			//this.scrollToBottom();
		},
		//第一个界面用户提交用户名
		usernameSubmit:function(){
			var username = d.getElementById("username_lt").value;
			if(username != ""){
				d.getElementById("username_lt").value = '';
				d.getElementById("loginbox").style.display = 'none';
				d.getElementById("chatbox").style.display = 'block';
				this.init(username);
			}
			return false;
		},
		init:function(username){
			/*
			客户端根据时间和随机数生成uid,这样使得聊天室用户名称可以重复。
			实际项目中，如果是需要用户登录，那么直接采用用户的uid来做标识就可以
			*/
			this.userid = this.genUid();
			this.username = username;
			
			d.getElementById("showusername").innerHTML = this.username;
			//this.msgObj.style.minHeight = (this.screenheight - db.clientHeight + this.msgObj.clientHeight) + "px";
			//this.scrollToBottom();
			
			//连接websocket后端服务器
			this.socket = io.connect('ws://192.168.2.204:3000');
			
			//告诉服务器端有用户登录
			this.socket.emit('login', {userid:this.userid, username:this.username});
			
			//监听新用户登录
			this.socket.on('login', function(o){
				CHAT.updateSysMsg(o, 'login');	

			});
			
			//监听用户退出
			this.socket.on('logout', function(o){
				CHAT.updateSysMsg(o, 'logout');
			});
			
			//监听消息发送
			this.socket.on('message', function(obj){
				CHAT.sendMsg(obj);
			});

		}
	};
	//通过“回车”提交用户名
	d.getElementById("username_lt").onkeydown = function(e) {
		e = e || event;
		if (e.keyCode === 13) {
			CHAT.usernameSubmit();
		}
	};
	//通过“回车”提交信息
	d.getElementById("content").onkeydown = function(e) {
		e = e || event;
		if (e.keyCode === 13) {
			CHAT.submit();
		}
	};
})();


//拖动聊天室
var dragFlag_lts = false;
var xlt,ylt;
jQuery(document).ready(function ($) {
    $("#mainDiv").mousedown(function (e) {
        dragFlag_lts = true;
        xlt = e.pageX - parseInt($("#mainDiv2").css("left"));
        ylt = e.pageY - parseInt($("#mainDiv2").css("top"));
     });
    $(document).mousemove(function (e) {
        if (dragFlag_lts) {
            var x = e.pageX - xlt; 
            var y = e.pageY - ylt;
            var wx = $(window).width() - $('#mainDiv2').width();
            var dy = $(document).height() - $('#mainDiv2').height();
            if(x >= 0 && x <= wx && y > 0 && y <= dy) {
                $("#mainDiv2").css({
                    top: y,
                    left: x
                }); //控件新位置
            ismove = true;
            }
        }
    }).mouseup(function () {
        dragFlag_lts = false;
    });
});
