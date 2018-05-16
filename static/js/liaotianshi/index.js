var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var redis=require('redis')
var querystring=require('querystring');

var client=redis.createClient();

// app.get('/', function(req, res){
// 	res.send('<h1>Welcome Realtime Server</h1>');
// });

var is_tupian='';

var formidable = require('formidable'),
    fs = require('fs'),
    TITLE = 'formidable上传',
    AVATAR_UPLOAD_FOLDER = '/avatar/',
    domain = "http://localhost:8000";

/* 图片上传路由 */
app.post('/uploader', function(req, res) {

//  var form = new formidable.IncomingForm();   //创建上传表单
//  form.encoding = 'utf-8';        //设置编辑
//  form.uploadDir = 'public' + AVATAR_UPLOAD_FOLDER;     //设置上传目录
//  form.keepExtensions = true;     //保留后缀
//  form.maxFieldsSize = 2 * 1024 * 1024;   //文件大小
//
//  form.parse(req, function(err, fields, files) {
//
//    if (err) {
//      res.locals.error = err;
//      res.render('index', { title: TITLE });
//      return;
//    }
//    //console.log(files);
//
//    var extName = '';  //后缀名
//    switch (files.fulAvatar.type) {
//      case 'image/pjpeg':
//        extName = 'jpg';
//        break;
//      case 'image/jpeg':
//        extName = 'jpg';
//        break;
//      case 'image/png':
//        extName = 'png';
//        break;
//      case 'image/x-png':
//        extName = 'png';
//        break;
//    }
//
//    if(extName.length == 0){
//      res.locals.error = '只支持png和jpg格式图片';
//      res.render('index', { title: TITLE });
//      return;
//    }
    extName="png";
    var avatarName = Math.random() + '.' + extName;
    //图片写入地址；
    var newPath = "public/avatar/"+avatarName//form.uploadDir + avatarName;
    //显示地址；
    var showUrl = "/static/js/liaotianshi/public" + AVATAR_UPLOAD_FOLDER + avatarName;
    //console.log("newPath",newPath);
    //fs.renameSync(files.fulAvatar.path, newPath);  //重命名
    //console.log("showUrl",showUrl);
    is_tupian=showUrl;
    // res.json({
    //   "newPath":showUrl
    // });
  //});


  //暂存请求体信息
    var body = "";

    //每当接收到请求体数据，累加到post中
    req.on('data', function (chunk) {
        body += chunk;  //一定要使用+=，如果body=chunk，因为请求favicon.ico，body会等于{}
    });

    //在end事件触发后，通过querystring.parse将post解析为真正的POST请求格式，然后向客户端返回。
    req.on('end', function () {
        // 解析参数
        body = querystring.parse(body);  //将一个字符串反序列化为一个对象
        formData=body.formData;
        //is_tupian=formData;
        //过滤data:URL  data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAABbMAAAOeCAYAAAA5mY3EAAAgAElEQVR4nOzdXWx
        var base64Data = formData.replace(/^data:image\/\w+;base64,/, "");
        base64Data = base64Data.replace(/ /g,'+');
        var dataBuffer = new Buffer(base64Data, 'base64');
        fs.writeFile(newPath, dataBuffer, function(err) {
            if(err){
              //res.send(err);
              console.log(err);
            }else{
              console.log("保存成功！");
              console.log(base64Data.slice(-80));
            }
        });
            //res.end();
    });



  //接收前台POST过来的base64

});


client.on('error',function(err){
	console.log('Error. '+err);
});

//在线用户
var onlineUsers = {};
//当前在线人数
var onlineCount = {};
//聊天记录
var record = new Array();
//消息条数
var ind = 0;
//userid 对应的 聊天室
var uid_lts = {};
//所有聊天室
var all_lts = new Array();
io.on('connection', function(socket){
	console.log('a user connected');
	
	//监听新用户加入
	socket.on('login', function(obj){
		//将新加入用户的唯一标识当作socket的名称，后面退出的时候会用到
		socket.name = obj.userid;
		//聊天室名称
		var ltsName = obj.liaotianshiName;
		uid_lts[obj.userid] = ltsName;

		if(!onlineUsers[ltsName]){
		    onlineUsers[ltsName] = {};
		}
		if(!onlineCount[ltsName]){
		    onlineCount[ltsName] = 0;
		}
		//检查在线列表，如果不在里面就加入
		if(!onlineUsers[ltsName].hasOwnProperty(obj.userid)) {
			onlineUsers[ltsName][obj.userid] = obj.username;
			//在线人数+1
			onlineCount[ltsName]++;
		}
		if(all_lts.indexOf(ltsName)<0){
		    all_lts.push(ltsName);
            client.set("liaotianshiList",JSON.stringify(onlineCount),redis.print); //写入redis
		}
		
		//向所有客户端广播用户加入
		io.emit('login', {onlineUsers:onlineUsers, onlineCount:onlineCount, user:obj,record:record});
		//console.log(obj.username+'加入了聊天室  '+ltsName);
	});
	
	//监听用户退出
	socket.on('disconnect', function(){
        var ltsName = uid_lts[socket.name];
		//将退出的用户从在线列表中删除
		try{
            if(onlineUsers[ltsName].hasOwnProperty(socket.name)) {
                //退出用户的信息
                var obj = {userid:socket.name, username:onlineUsers[ltsName][socket.name], liaotianshiName:ltsName};

                //删除
                delete onlineUsers[ltsName][socket.name];
                delete uid_lts[socket.name];
                //在线人数-1
                onlineCount[ltsName]--;
                //删除聊天室
                if(onlineCount[ltsName] <= 0){
                    delete onlineUsers[ltsName];
                    delete onlineCount[ltsName];
                    client.set("liaotianshiList",JSON.stringify(onlineCount),redis.print); //写入redis
                }

                //向所有客户端广播用户退出
                io.emit('logout', {onlineUsers:onlineUsers, onlineCount:onlineCount, user:obj});
                //console.log(obj.username+'退出了聊天室  '+obj.liaotianshiName);
            }
		}catch(err){
            console.log("异常出现："+err);
		}
	});
	
	//监听用户发布聊天内容
	socket.on('message', function(obj){
		obj['dateTime']=new Date().toLocaleString();
		//向所有客户端广播发布的消息
		function sendMessage(){
			io.emit('message', obj);
			record[ind]=obj
			ind++;
			//console.log(obj.username+'说：'+obj.content);
			fs.appendFile('..\\..\\..\\log\\liaotianshi.txt',obj.liaotianshiName+"\t"+obj.username+"\t"+obj.content+"\r\n",'utf8',function(err){
                if(err) { console.log(err); }
			});
			is_tupian='';
		}
		if(obj.content=='tupian'){
				var sit=setInterval(function() {
					if(is_tupian!=''){
    					obj.content="<img src='"+is_tupian+"'/>";
    					sendMessage();
    					clearInterval(sit);
    				}
    				
				}, 100);
		}else{
			sendMessage();
		}
		
	});
  
});

http.listen(3000, function(){
	console.log('listening on *:3000');
});