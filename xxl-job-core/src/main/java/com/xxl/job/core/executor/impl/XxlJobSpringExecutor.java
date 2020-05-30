package com.xxl.job.core.executor.impl;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.handler.impl.MethodJobHandler;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * xxl-job executor (for spring)
 *
 * @author xuxueli 2018-11-01 09:24:52
 *
 *
 * 入口类
 *  + 通过写入参数，写入参数，创建bean来启动xxl-job
 *
 * 实现的接口
 *  + ApplicationContextAware：要求spring注入上下文，从而可以使用上下问的内容
 *  + InitializingBean：在参数注入完成后，调用初始化代码
 *  + DisposableBean：在类消耗前调用，释放资
 *
 * 继承
 *  + XxlJobExecutor：核心类
 */
public class XxlJobSpringExecutor extends XxlJobExecutor implements ApplicationContextAware, InitializingBean, DisposableBean {


    /**
     * 通过 参数Spring的后置处理函数来实现启动
     * 1. 初始化 定时任务类（已经废弃的方式）
     * 2. 初始化 定时任务方法
     * 3. 初始化 胶水语言类工厂
     *      + 用于任务可以通过胶水语言注入实例，动态生成和执行，而不是写死在代码里面
     *      + 需要一个工厂类，用于创建bean
     * 4. 启动 xxl-job执行
     * @throws Exception
     */
    // start
    @Override
    public void afterPropertiesSet() throws Exception {

        // init JobHandler Repository
        initJobHandlerRepository(applicationContext);

        // init JobHandler Repository (for method)
        initJobHandlerMethodRepository(applicationContext);

        // refresh GlueFactory
        GlueFactory.refreshInstance(1);

        // super start
        super.start();
    }

    // destroy
    @Override
    public void destroy() {
        super.destroy();
    }

    /**
     * 初始化 定时任务类（已经废弃的方式）
     * + 这种注册方式需要类包含{@link JobHandler}注解，并且继承{@link IJobHandler}
     * + 从上下文获取包含{@link JobHandler}注解的类
     * + 对于每个类，判断是否满足是任务处理类的条件
     *      + 类需要类包含{@link JobHandler}注解
     *      + 类继承{@link IJobHandler}
     * + 将其向上转型成{@link IJobHandler}
     * + 对其名称进行查询，防止名称重复
     * + 对其进行注册
     *
     * @param applicationContext
     */
    private void initJobHandlerRepository(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            return;
        }

        // init job handler action
        Map<String, Object> serviceBeanMap = applicationContext.getBeansWithAnnotation(JobHandler.class);

        if (serviceBeanMap != null && serviceBeanMap.size() > 0) {
            for (Object serviceBean : serviceBeanMap.values()) {
                if (serviceBean instanceof IJobHandler) {
                    String name = serviceBean.getClass().getAnnotation(JobHandler.class).value();
                    IJobHandler handler = (IJobHandler) serviceBean;
                    if (loadJobHandler(name) != null) {
                        throw new RuntimeException("xxl-job jobhandler[" + name + "] naming conflicts.");
                    }
                    registJobHandler(name, handler);
                }
            }
        }
    }

    /**
     * 初始化 定时任务方法
     *
     * + 遍历上下文的全部bean的全部方法，找到包含{@link XxlJob}的方法
     * + 通过注册的值，依次取出任务的名称、初始化方法、销毁方法
     * + 对其名称进行查询，防止名称重复
     * + 执行的方法必须满足如下条件
     *      + 有且仅有一个类型为String的参数
     *      + 返回类型比如为{@link ReturnT}的子类
     * + 如果初始化方法、销毁方法的注解不为空，那么在当前bean中根据名称搜索对应的方法，获取对应的句柄
     * + 将获取的 执行、初始化、销毁方法、以及对于的方法的实例bean封装成一个执行任务
     * + 将执行任务类，根据名称注册到仓库
     *
     * @param applicationContext
     */
    private void initJobHandlerMethodRepository(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            return;
        }

        // init job handler from method
        String[] beanDefinitionNames = applicationContext.getBeanDefinitionNames();
        if (beanDefinitionNames!=null && beanDefinitionNames.length>0) {
            for (String beanDefinitionName : beanDefinitionNames) {
                Object bean = applicationContext.getBean(beanDefinitionName);
                Method[] methods = bean.getClass().getDeclaredMethods();
                for (Method method: methods) {
                    XxlJob xxlJob = AnnotationUtils.findAnnotation(method, XxlJob.class);
                    if (xxlJob != null) {

                        // name
                        String name = xxlJob.value();
                        if (name.trim().length() == 0) {
                            throw new RuntimeException("xxl-job method-jobhandler name invalid, for[" + bean.getClass() + "#"+ method.getName() +"] .");
                        }
                        if (loadJobHandler(name) != null) {
                            throw new RuntimeException("xxl-job jobhandler[" + name + "] naming conflicts.");
                        }

                        // execute method
                        if (!(method.getParameterTypes()!=null && method.getParameterTypes().length==1 && method.getParameterTypes()[0].isAssignableFrom(String.class))) {
                            throw new RuntimeException("xxl-job method-jobhandler param-classtype invalid, for[" + bean.getClass() + "#"+ method.getName() +"] , " +
                                    "The correct method format like \" public ReturnT<String> execute(String param) \" .");
                        }
                        if (!method.getReturnType().isAssignableFrom(ReturnT.class)) {
                            throw new RuntimeException("xxl-job method-jobhandler return-classtype invalid, for[" + bean.getClass() + "#"+ method.getName() +"] , " +
                                    "The correct method format like \" public ReturnT<String> execute(String param) \" .");
                        }
                        method.setAccessible(true);

                        // init and destory
                        Method initMethod = null;
                        Method destroyMethod = null;

                        if(xxlJob.init().trim().length() > 0) {
                            try {
                                initMethod = bean.getClass().getDeclaredMethod(xxlJob.init());
                                initMethod.setAccessible(true);
                            } catch (NoSuchMethodException e) {
                                throw new RuntimeException("xxl-job method-jobhandler initMethod invalid, for[" + bean.getClass() + "#"+ method.getName() +"] .");
                            }
                        }
                        if(xxlJob.destroy().trim().length() > 0) {
                            try {
                                destroyMethod = bean.getClass().getDeclaredMethod(xxlJob.destroy());
                                destroyMethod.setAccessible(true);
                            } catch (NoSuchMethodException e) {
                                throw new RuntimeException("xxl-job method-jobhandler destroyMethod invalid, for[" + bean.getClass() + "#"+ method.getName() +"] .");
                            }
                        }

                        // registry jobhandler
                        registJobHandler(name, new MethodJobHandler(bean, method, initMethod, destroyMethod));
                    }
                }
            }
        }

    }

    // ---------------------- applicationContext ----------------------
    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

}
