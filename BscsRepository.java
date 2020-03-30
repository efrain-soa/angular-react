package pe.com.claro.post.fullclaro.domain.repository;

import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.jdbc.Work;

import pe.com.claro.common.domain.repository.AbstractRepositorio;
import pe.com.claro.common.exception.DBException;
import pe.com.claro.common.property.Constantes;
import pe.com.claro.common.util.ClaroUtil;
import pe.com.claro.common.util.PropertiesExternos;
import pe.com.claro.post.fullclaro.canonical.request.ValidarClienteRequestType;


@Stateless
public class BscsRepository extends AbstractRepositorio<Object> implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(BscsRepository.class);
	XMLGregorianCalendar xmlGregorianCalendar;

	@Override
	protected Predicate[] getSearchPredicates(Root<Object> root, Object example) {
		// TODO Auto-generated method stub
		return null;
	}

	@PersistenceContext(unitName = Constantes.PERSISTENCE_CONTEXT_BSCS7)
	public void setPersistenceUnit00(final EntityManager em) {
		this.entityManager = em;
	}

	private void errorGenerico(Exception ex, String msjTx, String sp, String bd, PropertiesExternos propertiesExternos)
			throws DBException {

		logger.error(msjTx + Constantes.ERROR_EJECUCION_SP, ex);

		String error = ex + Constantes.TEXTOVACIO;
		String codError = Constantes.TEXTOVACIO;
		String msjError = Constantes.TEXTOVACIO;
		String msjDeveloper = ex.toString();

		if (error.toUpperCase(Locale.getDefault())
				.contains(Constantes.TIMEOUTEXCEPTION.toUpperCase(Locale.getDefault()))) {
			codError = propertiesExternos.procedure_codigo_error_idt1;
			msjError = propertiesExternos.procedure_mensaje_error_idt1;

		} else if (error.toUpperCase(Locale.getDefault())
				.contains(Constantes.PERSISTENCEEXCEPTION.toUpperCase(Locale.getDefault()))
				|| error.toUpperCase(Locale.getDefault())
						.contains(Constantes.HIBERNATEJDBCEXCEPTION.toUpperCase(Locale.getDefault()))
				|| error.toUpperCase(Locale.getDefault())
						.contains(Constantes.GENERICJDBCEXCEPTION.toUpperCase(Locale.getDefault()))) {
			codError = propertiesExternos.procedure_codigo_error_idt2;
			msjError = propertiesExternos.procedure_mensaje_error_idt2;

			if (ex.getCause() != null && ex.getCause() != null && ex.getCause() instanceof SQLException) {
				SQLException se = (SQLException) ex.getCause();
				msjDeveloper = Constantes.CODE + se.getErrorCode() + Constantes.MSG + se.getMessage() + Constantes.TRACE
						+ se.toString();
			}

		} else {
			codError = propertiesExternos.procedure_codigo_error_idt3;
			msjError = propertiesExternos.procedure_mensaje_error_idt3;
		}
		throw new DBException(codError,
				msjError.replace(Constantes.VARIABLE_DB, bd).replace(Constantes.VARIABLE_SP, sp), msjDeveloper, ex, 0);
	}

	
	public int funcionValidarFija(String msjTx, ValidarClienteRequestType request, PropertiesExternos prop)
			throws DBException {
		String nombreMetodo = "funcionValidarFija";
		logger.info(msjTx + Constantes.INICIO_METODO + nombreMetodo);
		int cantidadLineasFijas=0;
		StringBuffer storedProcedure = new StringBuffer();

		String nombreDB = prop.DB_BSCSDB_NOMBRE;//
		String owner = prop.DB_BSCSDB_OWNER;
		String nombreSP = prop.DB_BSCSDB_PKG_BONOS_FULLCLARO + Constantes.SEPARADORPUNTO + prop.DB_BSCSDB_SP_BSCSFUN_VAL_FIJA;

		logger.info(msjTx + "Consultando a la BD: " + nombreDB);
		logger.info(msjTx + "OWNER: " + owner);
		logger.info(msjTx + "STORE PROCEDURE: " + nombreSP);

		try {
			storedProcedure.append(ClaroUtil.getStoredProcedureByParameters(owner, nombreSP));
			logger.info(msjTx + "Consultando a la DB: " + nombreDB);
			logger.info(msjTx + "Ejecutando SP: " + storedProcedure);
			logger.info(msjTx + "Parametros SP [Input] : ");
			logger.info(msjTx + "[PI_DOCUMENTO] = " + request.getTipoDocumento());
			

			entityManager.unwrap(Session.class).doWork(new Work() {
				@Override
				public void execute(final Connection connection) throws SQLException {
					funcionValidarFija(msjTx, storedProcedure.toString(), connection, request, cantidadLineasFijas, prop);
				}
			});

		} catch (Exception ex) {
			this.errorGenerico(ex, msjTx, storedProcedure.toString(), nombreDB, prop);

		}
		return cantidadLineasFijas;
	}

	private void funcionValidarFija(String msjTx, String storedProcedure, Connection connection, ValidarClienteRequestType request,
			 int cantidadLineasFijas, PropertiesExternos propertiesExternos) throws SQLException {
		logger.info(msjTx + Constantes.EJECUTAR_SP);
		long tiempoInicioSP = System.currentTimeMillis();
		CallableStatement call = null;

		try {
			

			
			call = connection.prepareCall("{ ? = call "+storedProcedure+"( ? ) }");
			//call = connection.prepareCall("call " + storedProcedure + " (?,?)");
			logger.info(msjTx +"{ ? = call "+storedProcedure+"( ? ) }");
			call.setQueryTimeout(Integer.parseInt(propertiesExternos.DB_BSCSDB_TIMEOUT));
			call.registerOutParameter(1, Types.NUMERIC);
			call.setString(2, request.getNumeroDocumento());
			
			
			call.execute();

			logger.info(msjTx + Constantes.TIEMPO_TOTAL_SP + (System.currentTimeMillis() - tiempoInicioSP));
			logger.info(msjTx + Constantes.INVOCO_SP);
			logger.info(msjTx + Constantes.PARAMETROSSALIDA);

			cantidadLineasFijas = call.getInt(1);

		} catch (Exception e) {
			logger.error(Constantes.ERROR_EJECUCION_SP, e);

		} finally {
			if (call != null) {
				call.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}

	
	
	//movil
	

	public int funcionValidarMovil(String msjTx, ValidarClienteRequestType request, PropertiesExternos prop)
			throws DBException {
		String nombreMetodo = "funcionValidarMovil";
		logger.info(msjTx + Constantes.INICIO_METODO + nombreMetodo);
		int cantidadLineasMovil = 0;
		StringBuffer storedProcedure = new StringBuffer();

		String nombreDB = prop.DB_BSCSDB_NOMBRE;//
		String owner = prop.DB_BSCSDB_OWNER;
		String nombreSP = prop.DB_BSCSDB_PKG_BONOS_FULLCLARO + Constantes.SEPARADORPUNTO + prop.DB_BSCSDB_SP_BSCSFUN_VAL_MOVIL;

		logger.info(msjTx + "Consultando a la BD: " + nombreDB);
		logger.info(msjTx + "OWNER: " + owner);
		logger.info(msjTx + "STORE PROCEDURE: " + nombreSP);

		try {
			storedProcedure.append(ClaroUtil.getStoredProcedureByParameters(owner, nombreSP));
			logger.info(msjTx + "Consultando a la DB: " + nombreDB);
			logger.info(msjTx + "Ejecutando SP: " + storedProcedure);
			logger.info(msjTx + "Parametros SP [Input] : ");
			logger.info(msjTx + "[PI_DOCUMENTO] = " + request.getNumeroDocumento());

			entityManager.unwrap(Session.class).doWork(new Work() {
				@Override
				public void execute(final Connection connection) throws SQLException {
					funcionValidarMovil(msjTx, storedProcedure.toString(), connection, request, cantidadLineasMovil, prop);
				}
			});

		} catch (Exception ex) {
			this.errorGenerico(ex, msjTx, storedProcedure.toString(), nombreDB, prop);

		}
		return cantidadLineasMovil;
	}

	private void funcionValidarMovil(String msjTx, String storedProcedure, Connection connection, ValidarClienteRequestType request,
			 int cantidadLineasMovil, PropertiesExternos propertiesExternos) throws SQLException {
		logger.info(msjTx + Constantes.EJECUTAR_SP);
		long tiempoInicioSP = System.currentTimeMillis();
		CallableStatement call = null;

		try {

			call = connection.prepareCall("{ ? = call "+storedProcedure+"( ? ) }");
			//call = connection.prepareCall("call " + storedProcedure + " (?,?)");
			logger.info(msjTx +"{ ? = call "+storedProcedure+"( ? ) }");
			call.setQueryTimeout(Integer.parseInt(propertiesExternos.DB_BSCSDB_TIMEOUT));
			call.registerOutParameter(1, Types.NUMERIC);
			call.setString(2, request.getNumeroDocumento());
			
			
			call.execute();

			logger.info(msjTx + Constantes.TIEMPO_TOTAL_SP + (System.currentTimeMillis() - tiempoInicioSP));
			logger.info(msjTx + Constantes.INVOCO_SP);
			logger.info(msjTx + Constantes.PARAMETROSSALIDA);

			cantidadLineasMovil = call.getInt(1);

		} catch (Exception e) {
			logger.error(Constantes.ERROR_EJECUCION_SP, e);

		} finally {
			if (call != null) {
				call.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}


8	public void metodo1(name){
	
	logger.info(name+"Entrando al metodo1");
     }
	
	
}

