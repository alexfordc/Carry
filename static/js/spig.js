/* spig.js */
//右键菜单
jQuery(document).ready(function ($) {
    $("#spig").mousedown(function (e) {
        if(e.which==3){
        showMessage("秘密通道:<br /><a href=\"/tj\" title=\"统计表\">统计表</a>    <a href=\"/\" title=\"首页\">首页</a>",10000);
}
});
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
        if (now > 0 && now <= 6) {
            showMessage( ' 你是夜猫子呀？还不睡觉，明天起的来么你？', 6000);
        } else if (now > 6 && now <= 11) {
            showMessage( ' 早上好！欢迎来到凯瑞投资有限公司！', 6000);
        } else if (now > 11 && now <= 14) {
            showMessage( ' 中午好！人是铁，饭是钢，别忘了吃饭呀！', 6000);
        } else if (now > 14 && now <= 18) {
            showMessage( ' 下午好！欢迎来到凯瑞投资有限公司！', 6000);
        } else {
            showMessage( ' 我守住黄昏，守过夜晚！', 6000);
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
    $('h2 a').click(function () {//标题被点击时
        showMessage('正在用吃奶的劲加载《<span style="color:#0099cc;">' + $(this).text() + '</span>》请稍候');
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

//无聊讲点什么
jQuery(document).ready(function ($) {
    window.setInterval(function () {
        msgs = ["播报天气<iframe name='weather_inc' src='http://i.tianqi.com/index.php?c=code&id=7' style='border:solid 1px red' width='225' height='90' frameborder='0' marginwidth='0' marginheight='0' scrolling='no'></iframe>", "莫因寂寞难耐的等待而入市！", "", "想要在期货市场混下去，必须自信，要相信自己的判断！", "接受失败等于向成功迈出了一步", "不管亏损多少，都要保持旺盛的斗志", "我可爱吧！嘻嘻!~^_^!~~","始终遵守你自己的投资计划的规则，这将加强良好的自我控制！~~","上山爬坡缓慢走，烘云托月是小牛。"];
        var i = Math.floor(Math.random() * msgs.length);
        showMessage(msgs[i], 10000);
    }, 35000);
});

//无聊动动
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
			    duration: 2000,
			    complete: showMessage(msgs[i])
			});
    }, 45000);
});

//评论资料
jQuery(document).ready(function ($) {
    $("#author").click(function () {
        showMessage("留下你的尊姓大名！");
        $(".spig").animate({
            top: $("#author").offset().top - 70,
            left: $("#author").offset().left - 170
        },
		{
		    queue: false,
		    duration: 1000
		});
    });
    $("#email").click(function () {
        showMessage("留下你的邮箱，不然就是无头像人士了！");
        $(".spig").animate({
            top: $("#email").offset().top - 70,
            left: $("#email").offset().left - 170
        },
		{
		    queue: false,
		    duration: 1000
		});
    });
    $("#url").click(function () {

        showMessage("快快告诉我你的家在哪里，好让我去参观参观！");
        $(".spig").animate({
            top: $("#url").offset().top - 70,
            left: $("#url").offset().left - 170
        },
		{
		    queue: false,
		    duration: 1000
		});
    });
    $("#comment").click(function () {
        showMessage("认真填写哦！不然会被认作垃圾评论的！我的乖乖~");
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

//鼠标点击时
jQuery(document).ready(function ($) {
    var stat_click = 0;
    $(".mumu").click(function () {
        if (!ismove) {
            stat_click++;
            if (stat_click > 4) {
                msgs = ["循序渐进，精益求精。", "反弹不是底，是底不反弹", "进场容易出场难"];
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