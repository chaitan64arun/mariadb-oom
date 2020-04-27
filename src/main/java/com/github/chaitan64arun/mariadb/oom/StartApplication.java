package com.github.chaitan64arun.mariadb.oom;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.transaction.annotation.Transactional;

@SpringBootApplication
public class StartApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(StartApplication.class);

	@Autowired
	DataSource dataSource;

	@Autowired
	JdbcTemplate jdbcTemplate;

	public static void main(String[] args) {
		SpringApplication.run(StartApplication.class, args);
	}
	

	@Transactional // This is important
	@Override
	public void run(String... args) throws Exception {

		String sql1 = "select * from salaries;"; // Contains 3 Million records
		String sql2 = "select * from departments;";

		log.info("Our Connection is = " + dataSource.getConnection());

		log.info("Starting application");

		jdbcTemplate.setFetchSize(100);
		log.info("FetchSize is {}", jdbcTemplate.getFetchSize());

		
		// Sleep for 10s for me to check Visual VM
		Thread.currentThread().sleep(10000);
		
		// Get a JDBC connection

		// Execute a query and get resultset iterator

		jdbcTemplate.query(sql1, new RowCallbackHandler() {
			public void processRow(ResultSet resultSet) throws SQLException {
				
				int count = 1;
				
				while (resultSet.next()) {
					// log.info("Employee No: {}, Salary: {}", resultSet.getObject(1), resultSet.getObject(2));
					try {
						Thread.currentThread().sleep(20);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					if(count % 1000 == 0) {
						// Execute second query
						log.info("Second one executed");
						List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql2);
						for (Map row : rows) {
							//log.info("Dept No: {}, Dept Name: {}", row.get("dept_no"), row.get("dept_name"));

						}						
					}
					count++;
				}
			}
		});

	}

}
