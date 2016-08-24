package io.infoworks.util;

import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;
import io.infoworks.ingestion.model.TableGroupInfo;

public class TemplateUtil {
	
	private Logger logger = Logger.getLogger("TemplateUtil") ;
	
	
	public String getTableGrpFromTemplate(String templateName, TableGroupInfo gInfo) throws Exception {
		// 1. Configure FreeMarker
		//
		// You should do this ONLY ONCE, when your application starts,
		// then reuse the same Configuration object elsewhere.

		Configuration cfg = new Configuration();
		// Where do we load the templates from:
		cfg.setClassForTemplateLoading(this.getClass(), "templates");

		// Some other recommended settings:
		cfg.setIncompatibleImprovements(new Version(2, 3, 20));
		cfg.setDefaultEncoding("UTF-8");
		cfg.setLocale(Locale.US);
		cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

		// 2. Proccess template(s)
		//
		// You will do this for several times in typical applications.

		// 2.1. Prepare the template input:

		Map<String, Object> input = new HashMap<String, Object>();
		input.put("tableids",gInfo.getTableIds());

		// 2.2
		//Template template = cfg.getTemplate("basicTableGroup.tpl");
		Template template = cfg.getTemplate(templateName);
		
		// 2.3. Generate the output
		//  write output into a string:
		Writer stringWriter = new StringWriter() ;// new FileWriter(new File("output.html"));
		try {
			template.process(input, stringWriter);
		} finally {
			stringWriter.close();
		}

		logger.info("\n data is  :" + stringWriter);
		return stringWriter.toString() ;
	}
	
	public static void main(String[] args) throws Exception {
		TemplateUtil util = new TemplateUtil() ;
		List<String> ids = new ArrayList<String>() ;
		ids.add("1") ;
		ids.add("2") ;
		ids.add("3") ;
		TableGroupInfo info = new TableGroupInfo() ;
		info.setTableIds(ids);
		
		util.getTableGrpFromTemplate("basicTableGroup.tpl", info); 
	}
}
