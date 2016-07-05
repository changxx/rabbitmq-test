素材工程说明：

<ul>
    <li>表一律以matter_开头。</li>
    <li>因素材种类较多，不同业务严格按包名区分，同一业务的代码写在一个包下，不允许存在业务代码不分包的情况。</li>
    <li>api返回实体以DTO结尾</li>
    <li>
        工程依赖关系如下：
        <img src="http://haitao.nosdn1.127.net/iq91cpx018_1082_746.jpg"/>

        haitao-matter-api    haitao-matter-service
                    |            |
                haitao-matter-provider
                          |
                  haitao-matter-web
    </li>
</ul>






# rabbitmq-test
rabbitmq-test
