/* spig.js */
//右键菜单
var showmessages="常用功能:<br><span style=\"line-height:30px;\" ><a style=\"color:red;\" href=\"/\" title=\"首页\">首页</a>&nbsp;&nbsp;<a style=\"color:green;\" href=\"/tj\" title=\"统计表\">统计表</a>&nbsp;&nbsp;<a style=\"color:blue;\" href=\"#\" onclick=\"javascript:xinwen_show()\">新闻</a> "+
            "</span><br><a style=\"color:red;\" href=\"/zt\">柱图</a>&nbsp;&nbsp;<a style=\"color:green;\" href=\"/zx\">折线图</a>&nbsp;&nbsp;<a style=\"color:blue;\" id=\"liaotian\" href=\"#\" title=\"聊天\">聊天</a>"
jQuery(document).ready(function ($) {
    $("#spig").mousedown(function (e) {
        if(e.which==3){
        showMessage(showmessages,20000);
    }

$("#liaotian").click(function(){
        var div=$("#liaotian_div")
        div.css('display','block');
        document.getElementById('liaotian_textarea').focus();
        div.hide().stop();
        div.fadeIn();
    	div.fadeTo("1", 1);
    	div.fadeOut(30000);
	});
});
function bfsy_ajax(){
    var text=$("#liaotian_textarea");
    var mesg=text.val().trim();
        if(mesg.length>0){
        //$.get('/bfsy',{"msg":text.val()},callBack);
        $.ajax({
				type:"GET",
				url:"/bfsy",
				data:{"msg":mesg},
				success:callBack
			});
			$('#chatAudio')[0].play(); //播放声音
			$("#chatAudio").attr('src','');
			text.val('');
			function callBack(answer){
				if(answer.length>0){
				    showMessage(answer,2400*answer.length);
					$("#chatAudio").attr('src','https://ss0.baidu.com/6KAZsjip0QIZ8tyhnq/text2audio?tex='+answer+'&cuid=dict&lan=ZH&ctp=1&pdt=30&vol=9&spd=4');
					$('#chatAudio')[0].play(); //播放声音
				}
			}
		}
}
document.onkeydown = function (evt) {//监听键盘敲击
            evt = evt ? evt : window.event;
            if (evt.keyCode == 13) { //按下Enter键
                //判断光标是否聚焦在此,  KEY 入数量
                if ($("#liaotian_textarea").is(":focus")) {
                    bfsy_ajax();
                }
            }
            else if (evt.keyCode == 8 && event.srcElement.readOnly == true) {//防止backspace键在input readOnly产生回退页面
                evt.keyCode = 0;
                return false;
            }
        }
$("#liaotian_submit").click(function(){
        var div=$("#liaotian_div");
        bfsy_ajax();
        document.getElementById('liaotian_textarea').focus();
        div.hide().stop();
    	div.fadeIn();
    	div.fadeTo("1", 1);
    	div.fadeOut(30000);
});
$("#liaotian_textarea").click(function(){
	var div=$("#liaotian_div");
	div.hide().stop();
    div.fadeIn();
    div.fadeOut(30000);
})
$("#spig").bind("contextmenu", function(e) {
    return false;
});
});

//鼠标在消息上时
jQuery(document).ready(function ($) {
    $("#message").hover(function () {
       $("#message").fadeTo("100", 1);
     });
});

//鼠标在上方时
jQuery(document).ready(function ($) {
    //$(".mumu").jrumble({rangeX: 2,rangeY: 2,rangeRot: 1});
    $(".mumu").mouseover(function () {
       $(".mumu").fadeTo("300", 0.3);
       msgs = ["我永远只能是个观察者，而不是个控制者", "我会隐身哦！嘿嘿！", "观注市场对新事件的反应比消息本身更有意义！", "当市场走势并不如愿时，趁早离场！"];
       var i = Math.floor(Math.random() * msgs.length);
        showMessage(msgs[i]);
    });
    $(".mumu").mouseout(function () {
        $(".mumu").fadeTo("300", 1)
    });
});

//开始
jQuery(document).ready(function ($) {
    if (is_show) { //如果要显示
        var now = (new Date()).getHours();
        if(is_show2=='/'){
        if (now > 0 && now <= 6) {
            showMessage( ' 黑用冷伪装坚强,夜以静隐忍苍凉！', 6000);
        } else if (now > 6 && now < 11) {
            showMessage( ' 上午好！欢迎来到凯瑞投资有限公司！', 6000);
        } else if (now >= 11 && now <= 12) {
            showMessage( ' 中午好！人是铁，饭是钢，别忘了吃饭呀！', 6000);
        } else if (now > 12 && now <= 18) {
            showMessage( ' 下午好！欢迎来到凯瑞投资有限公司！', 6000);
        } else {
            showMessage( ' 我守住黄昏，守过夜晚！', 6000);
        }
    }else{
        var msgs=["观望也是持仓","下单止损紧跟随","绝对不要加死码","抢顶和抢底要当心","止损既设，万不可悔","市场聒噪，乱我心事","苦心孤诣，深研市场","耐心比决心重要","计划你的交易，交易你的计划","投资市道清淡的市场是危险的",
        "期货投资人善于做一些不当的研究","事前须三思，临阵要严格按照计划行事","要不断培养耐心、坚韧、果断、理智的优良品质","成功的交易者是技巧、心态和德行的统一，三者不可分离","亏损可以使人谦虚，盈利可以使人骄傲。失败是成功的摇篮"];
        var i = Math.floor(Math.random() * msgs.length);
        showMessage(msgs[i],6000);
    }
    }
    else {
        showMessage('欢迎' + '来到《' + title + '》', 6000);
    }
    $(".spig").animate({
        top: $(".spig").offset().top + 300,
        left: document.body.offsetWidth - 160
    },
	{
	    queue: false,
	    duration: 1000
	});
});

//鼠标在某些元素上方时
jQuery(document).ready(function ($) {
    $('li a').click(function () {//标题被点击时
        showMessage('正在努力加载《<span style="color:#0099cc;">' + $(this).text() + '</span>》请稍候');
    });
    $('h2 a').mouseover(function () {
        showMessage('要看看《<span style="color:#0099cc;">' + $(this).text() + '</span>》公司么？');
    });
    $('#index').mouseover(function(){
        showMessage('要进入首页吗?');
    });
    $('#stockDatas_msg').mouseover(function(){
        showMessage('股票信息<br>可以对股票进行检索以及图形展示');
    });
    $('#zhutu_msg').mouseover(function(){
        showMessage('柱状图<br>恒生指数权重股，以柱状图实时显示');
    });
    $('#zhexian_msg').mouseover(function(){
        showMessage('折线图<br>恒生指数权重股，以折线图实时显示当天贡献的点数信息');
    });
    $('#zhexian2_msg').mouseover(function(){
        showMessage('折线图<br>恒生指数权重股，以折线图实时显示当天贡献的点数信息');
    });
    $('#kline_msg').mouseover(function(){
        showMessage('K线图<br>K线图展示恒生指数期货行情');
    });
    $('#tongji_msg').mouseover(function(){
        showMessage('交易统计表<br>历史交易统计，表格汇总');
    });
    $('#moni_msg').mouseover(function(){
        showMessage('模拟测试表<br>以历史行情为基础，进行模拟交易测试');
    });
    $('#tools_msg').mouseover(function(){
        showMessage('友情链接<br>一些常用的链接');
    });
    $('#today_msg').mouseover(function(){
        showMessage('已经涨停股票<br>当前已经涨停的股票');
    });
    $('#tomorrow_msg').mouseover(function(){
        showMessage('将要涨停股票<br>自查询日期开始，将要上涨的股票');
    });

});


jQuery(document).ready(function ($) {
    window.setInterval(function () {
        msgs = ["莫因寂寞难耐的等待而入市！", "流水不腐，户枢不蠹", "想要在期货市场混下去，必须自信，要相信自己的判断！", "接受失败等于向成功迈出了一步", "不管亏损多少，都要保持旺盛的斗志", "专业的交易者，他首先是个具备丰富内心世界和涵养的人!~^_^!~~","始终遵守你自己的投资计划的规则，这将加强良好的自我控制！~~","上山爬坡缓慢走，烘云托月是小牛。"];
        var i = Math.floor(Math.random() * msgs.length);
        showMessage(msgs[i], 20000);
    }, 40000);
});


jQuery(document).ready(function ($) {
    window.setInterval(function () {
        msgs = ["播报天气<iframe name='weather_inc' src='http://i.tianqi.com/index.php?c=code&id=7' style='border:solid 1px red' width='225' height='90' frameborder='0' marginwidth='0' marginheight='0' scrolling='no'></iframe>", "止损要牢记，亏损可减持！", "利好便卖，利空便买，成功人士，多善此举！", "把损失放在心上，利润就会照看好自己...", "收市便休兵，回家好休息", "避免频繁入市！~"];
        var i = Math.floor(Math.random() * msgs.length);
        s = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6,0.7,0.75,-0.1, -0.2, -0.3, -0.4, -0.5, -0.6,-0.7,-0.75];
        var i1 = Math.floor(Math.random() * s.length);
        var i2 = Math.floor(Math.random() * s.length);
            $(".spig").animate({
            left: document.body.offsetWidth/2*(1+s[i1]),
            top:  document.body.offsetHeight/2*(1+s[i1])
        },
			{
			    duration: 3000,
			    complete: showMessage(msgs[i])
			});
    }, 45000);
});

//点击事件提示
jQuery(document).ready(function ($) {
    $("#datetimes").click(function () {
        showMessage("选择开始日期");
        $(".spig").animate({
            top: $("#author").offset().top - 70,
            left: $("#author").offset().left - 170
        },
		{
		    queue: false,
		    duration: 1000
		});
    });
    $("#rq_ts").click(function () {
        showMessage("输入交易天数");
        $(".spig").animate({
            top: $("#email").offset().top - 70,
            left: $("#email").offset().left - 170
        },
		{
		    queue: false,
		    duration: 1000
		});
    });
    $("#select_id").click(function () {
        showMessage("选择ID号或者名称");
        $(".spig").animate({
            top: $("#url").offset().top - 70,
            left: $("#url").offset().left - 170
        },
		{
		    queue: false,
		    duration: 1000
		});
    });
    $("#select_fa").click(function () {
        showMessage("请选择方案");
        $(".spig").animate({
            top: $("#comment").offset().top - 70,
            left: $("#comment").offset().left - 170
        },
		{
		    queue: false,
		    duration: 1000
		});
    });
     $("#select_db").click(function () {
        showMessage("数据库选择<br>1： ticker数据 <br>2：同花顺数据");
        $(".spig").animate({
            top: $("#comment").offset().top - 70,
            left: $("#comment").offset().left - 170
        },
		{
		    queue: false,
		    duration: 1000
		});
    });
});

var spig_top = 50;
//滚动条移动
jQuery(document).ready(function ($) {
    var f = $(".spig").offset().top;
    $(window).scroll(function () {
        $(".spig").animate({
            top: $(window).scrollTop() + f +300
        },
		{
		    queue: false,
		    duration: 1000
		});
    });
});
//移动端与电脑端的判断
var client_pd='computer';
if(/Android|webOS|iPhone|iPod|BlackBerry/i.test(navigator.userAgent)){
	client_pd='sj';
}
//鼠标点击时
jQuery(document).ready(function ($) {
    var stat_click = 0;
    $(".mumu").click(function () {
        if(client_pd=='computer'){
        if (!ismove) {
            stat_click++;
            if (stat_click > 4) {
                msgs = ["循序渐进，精益求精", "反弹不是底，是底不反弹", "进场容易出场难", "任何时候忘记了去尊重市场，都会铸下大错"];
                var i = Math.floor(Math.random() * msgs.length);
                //showMessage(msgs[i]);
            } else {
                msgs = ["筋斗云！~我飞！", "高位十字星，我跑呀跑呀跑！~~", "会买的是徒弟，会卖的是师傅，会休息的是师爷！", "高位跳空向上走，神仙招手却不留 ", "不急功近利，不三心二意！", "任何投资都需具备智慧性的忍耐力！"];
                var i = Math.floor(Math.random() * msgs.length);
                //showMessage(msgs[i]);
            }
        s = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6,0.7,0.75,-0.1, -0.2, -0.3, -0.4, -0.5, -0.6,-0.7,-0.75];
        var i1 = Math.floor(Math.random() * s.length);
        var i2 = Math.floor(Math.random() * s.length);
            $(".spig").animate({
            left: document.body.offsetWidth/2*(1+s[i1]),
            top:  document.body.offsetHeight/2*(1+s[i1])
            },
			{
			    duration: 500,
			    complete: showMessage(msgs[i])
			});
        } else {
            ismove = false;
        }
        }else{
            showMessage(showmessages,20000);
        }
    });
});
//显示消息函数 
function showMessage(a, b) {
    if (b == null) b = 10000;
    jQuery("#message").hide().stop();
    jQuery("#message").html(a);
    jQuery("#message").fadeIn();
    jQuery("#message").fadeTo("1", 1);
    jQuery("#message").fadeOut(b);
};

//拖动
var _move = false;
var ismove = false; //移动标记
var _x, _y; //鼠标离控件左上角的相对位置
jQuery(document).ready(function ($) {
    $("#spig").mousedown(function (e) {
        _move = true;
        _x = e.pageX - parseInt($("#spig").css("left"));
        _y = e.pageY - parseInt($("#spig").css("top"));
     });
    $(document).mousemove(function (e) {
        if (_move) {
            var x = e.pageX - _x; 
            var y = e.pageY - _y;
            var wx = $(window).width() - $('#spig').width();
            var dy = $(document).height() - $('#spig').height();
            if(x >= 0 && x <= wx && y > 0 && y <= dy) {
                $("#spig").css({
                    top: y,
                    left: x
                }); //控件新位置
            ismove = true;
            }
        }
    }).mouseup(function () {
        _move = false;
    });
});

